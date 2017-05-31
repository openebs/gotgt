/*
Copyright 2016 The GoStor Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package iscsit

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	glog "github.com/Sirupsen/logrus"
	"github.com/openebs/gotgt/pkg/api"
	"github.com/openebs/gotgt/pkg/config"
	"github.com/openebs/gotgt/pkg/port"
	"github.com/openebs/gotgt/pkg/scsi"
	"github.com/openebs/gotgt/pkg/util"
)

type ISCSITargetService struct {
	SCSI         *scsi.SCSITargetService
	Name         string
	iSCSITargets map[string]*ISCSITarget
	done         chan bool
	stopFeed     chan bool
	txIO         chan Msg
	statsEnq     chan bool
	statsRes     chan port.Stats
	SCSIIOCount  map[int]int64
	listen       net.Listener
	state        uint8
	lock         *sync.RWMutex
}

func init() {
	port.RegisterTargetService("iscsi", NewISCSITargetService)
}

const (
	STATE_INIT = iota
	STATE_RUNNING
	STATE_SHUTTING_DOWN
	STATE_TERMINATE
)

func NewISCSITargetService(base *scsi.SCSITargetService) (port.SCSITargetService, error) {
	return &ISCSITargetService{
		Name:         "iscsi",
		iSCSITargets: map[string]*ISCSITarget{},
		SCSI:         base,
		done:         make(chan bool, 1),
		stopFeed:     make(chan bool, 1),
		txIO:         make(chan Msg, 100),
		statsEnq:     make(chan bool, 1),
		statsRes:     make(chan port.Stats, 1),
		SCSIIOCount:  map[int]int64{},
		state:        STATE_INIT,
		lock:         &sync.RWMutex{},
	}, nil
}

func (s *ISCSITargetService) getState() uint8 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.state
}

func (s *ISCSITargetService) setState(st uint8) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.state = st
}

func (s *ISCSITargetService) Stats() port.Stats {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.statsEnq <- true
	stats := <-s.statsRes
	stats.SCSIIOCount = s.SCSIIOCount
	return stats
}

func (s *ISCSITargetService) NewTarget(tgtName string, configInfo *config.Config) (port.SCSITargetDriver, error) {
	if _, ok := s.iSCSITargets[tgtName]; ok {
		return nil, fmt.Errorf("target name has been existed")
	}
	stgt, err := s.SCSI.NewSCSITarget(len(s.iSCSITargets), "iscsi", tgtName)
	if err != nil {
		return nil, err
	}
	tgt := newISCSITarget(stgt)
	s.iSCSITargets[tgtName] = tgt
	scsiTPG := tgt.SCSITarget.TargetPortGroups[0]
	targetConfig := configInfo.ISCSITargets[tgtName]
	for tpgt, portalIDArrary := range targetConfig.TPGTs {
		tpgtNumber, _ := strconv.ParseUint(tpgt, 10, 16)
		tgt.TPGTs[uint16(tpgtNumber)] = &iSCSITPGT{uint16(tpgtNumber), make(map[string]struct{})}
		targetPortName := fmt.Sprintf("%s,t,0x%02x", tgtName, tpgtNumber)
		scsiTPG.TargetPortGroup = append(scsiTPG.TargetPortGroup, &api.SCSITargetPort{uint16(tpgtNumber), targetPortName})
		for _, portalID := range portalIDArrary {
			portal := configInfo.ISCSIPortals[portalID]
			s.AddiSCSIPortal(tgtName, uint16(tpgtNumber), portal.Portal)
		}
	}
	return tgt, nil
}

func (s *ISCSITargetService) AddiSCSIPortal(tgtName string, tpgt uint16, portal string) error {
	var (
		ok       bool
		errMsg   string
		target   *ISCSITarget
		tpgtInfo *iSCSITPGT
	)

	if target, ok = s.iSCSITargets[tgtName]; !ok {
		errMsg = fmt.Sprintf("no target %s", tgtName)
		return errors.New(errMsg)
	}

	if tpgtInfo, ok = target.TPGTs[tpgt]; !ok {
		errMsg = fmt.Sprintf("no tpgt %d", tpgt)
		return errors.New(errMsg)
	}
	tgtPortals := tpgtInfo.Portals

	if _, ok = tgtPortals[portal]; !ok {
		tgtPortals[portal] = struct{}{}
	} else {
		errMsg := fmt.Sprintf("duplicate portal %s,in %s,%d", portal, tgtName, tpgt)
		return errors.New(errMsg)
	}

	return nil
}

func (s *ISCSITargetService) HasPortal(tgtName string, tpgt uint16, portal string) bool {
	var (
		ok       bool
		target   *ISCSITarget
		tpgtInfo *iSCSITPGT
	)

	if target, ok = s.iSCSITargets[tgtName]; !ok {
		return false
	}
	if tpgtInfo, ok = target.TPGTs[tpgt]; !ok {
		return false
	}
	tgtPortals := tpgtInfo.Portals

	if _, ok = tgtPortals[portal]; !ok {
		return false
	} else {
		return true
	}
}

// Stop the server by closing the listener
// todo: handling established connections
func (s *ISCSITargetService) Stop() error {
	if s.getState() != STATE_RUNNING {
		fmt.Printf("SKIP Closing Listener...  state is %v\n", s.getState())
		return nil
	}
	s.setState(STATE_SHUTTING_DOWN)
	s.done <- true

	fmt.Println("Closing Listener ...")
	s.listen.Close() // Accept will see an error
	s.setState(STATE_TERMINATE)

	return nil
}

func (s *ISCSITargetService) Run() error {
	var (
		err     error
		connNum int
	)
	for _, iSCSITarget := range s.iSCSITargets {
		for _, iSCSITPGT := range iSCSITarget.TPGTs {
			for portal, _ := range iSCSITPGT.Portals {
				if portal != "" {
					s.listen, err = net.Listen("tcp", portal)
				}
			}
		}
	}
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	defer func() {
		s.setState(STATE_SHUTTING_DOWN)
		s.done <- true
		s.listen.Close()
		s.setState(STATE_TERMINATE)
	}()
	s.setState(STATE_RUNNING)
	go s.StatsFeed()
	for {
		glog.Info("Listening ...")
		conn, err := s.listen.Accept()
		if err != nil {
			select {
			case <-s.done:
			default:
				glog.Error(err)

			}
			return err
		}
		glog.Info(conn.LocalAddr().String())
		glog.Info("Accepting ...")
		iscsiConn := &iscsiConnection{conn: conn}
		iscsiConn.init()
		iscsiConn.ConnNum = connNum
		connNum += 1
		iscsiConn.rxIOState = IOSTATE_RX_BHS

		glog.Infof("connection is connected from %s...\n", conn.RemoteAddr().String())
		// start a new thread to do with this command
		go s.handler(DATAIN, iscsiConn)
	}
	return nil
}

func (s *ISCSITargetService) handler(events byte, conn *iscsiConnection) {

	if events&DATAIN != 0 {
		glog.Debugf("rx handler processing...")
		go s.rxHandler(conn)
	}
	if conn.state != CONN_STATE_CLOSE && events&DATAOUT != 0 {
		glog.Debugf("tx handler processing...")
		s.txHandler(conn)
	}
	if conn.state == CONN_STATE_CLOSE {
		glog.Warningf("iscsi connection[%d] closed", conn.cid)
		conn.close()
	}
}

func (s *ISCSITargetService) rxHandler(conn *iscsiConnection) {
	var (
		hdigest uint = 0
		ddigest uint = 0
		final   bool = false
		cmd     *ISCSICommand
	)
	conn.readLock.Lock()
	defer conn.readLock.Unlock()
	if conn.state == CONN_STATE_SCSI {
		hdigest = conn.sessionParam[ISCSI_PARAM_HDRDGST_EN].Value & DIGEST_CRC32C
		ddigest = conn.sessionParam[ISCSI_PARAM_DATADGST_EN].Value & DIGEST_CRC32C
	}
	for {
		switch conn.rxIOState {
		case IOSTATE_RX_BHS:
			glog.Debugf("rx handler: IOSTATE_RX_BHS")
			buf, length, err := conn.readData(BHS_SIZE)
			if err != nil {
				glog.Error(err)
				return
			}
			if length == 0 {
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
				return
			}
			conn.rxBuffer = buf
			cmd, err = parseHeader(buf)
			if err != nil {
				glog.Error(err)
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
				return
			}
			conn.req = cmd
			if length == BHS_SIZE && cmd.DataLen != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_AHS
				break
			}
			glog.Debugf("got command: \n%s", cmd.String())
			glog.Debugf("got buffer: %v", buf)
			final = true
		case IOSTATE_RX_INIT_AHS:
			conn.rxIOState = IOSTATE_RX_DATA
			break
			if hdigest != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_HDIGEST
			}
		case IOSTATE_RX_DATA:
			if ddigest != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_DDIGEST
			}
			if cmd == nil {
				return
			}
			dl := ((cmd.DataLen + DataPadding - 1) / DataPadding) * DataPadding
			buf := []byte{}
			length := 0
			for length < dl {
				b, l, err := conn.readData(dl - length)
				if err != nil {
					glog.Error(err)
					return
				}
				length += l
				buf = append(buf, b...)
			}
			if length != dl {
				glog.Debugf("get length is %d, but expected %d", length, dl)
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
				return
			}
			cmd.RawData = buf[:length]
			conn.rxBuffer = append(conn.rxBuffer, buf...)
			final = true
			glog.Debugf("got command: \n%s", cmd.String())
		default:
			glog.Errorf("error %d %d\n", conn.state, conn.rxIOState)
			return
		}

		if final {
			break
		}
	}

	if conn.state == CONN_STATE_SCSI {
		s.scsiCommandHandler(conn)
	} else {
		conn.txIOState = IOSTATE_TX_BHS
		conn.resp = &ISCSICommand{}
		switch conn.req.OpCode {
		case OpLoginReq:
			glog.Debugf("OpLoginReq")
			if err := s.iscsiExecLogin(conn); err != nil {
				glog.Error(err)
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		case OpLogoutReq:
			glog.Debugf("OpLogoutReq")
			if err := iscsiExecLogout(conn); err != nil {
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		case OpTextReq:
			glog.Debugf("OpTextReq")
			if err := s.iscsiExecText(conn); err != nil {
				glog.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		default:
			iscsiExecReject(conn)
		}
		glog.Debugf("connection state is %v", conn.state)
		glog.Debugf("%#v", conn.resp.String())
		s.handler(DATAOUT, conn)
	}
}

func (s *ISCSITargetService) iscsiExecLogin(conn *iscsiConnection) error {
	var (
		target *ISCSITarget
		cmd    = conn.req
		TPGT   uint16
		err    error
	)
	conn.resp = &ISCSICommand{
		OpCode:   OpLoginResp,
		Transit:  true,
		CSG:      cmd.CSG,
		NSG:      FullFeaturePhase,
		StatSN:   cmd.ExpStatSN,
		TaskTag:  cmd.TaskTag,
		ExpCmdSN: cmd.CmdSN,
		MaxCmdSN: cmd.CmdSN,
		RawData: util.MarshalKVText([]util.KeyValue{
			{"HeaderDigest", "None"},
			{"DataDigest", "None"},
			{"ImmediateData", "Yes"},
			{"InitialR2T", "Yes"},
			{"MaxBurstLength", "262144"},
			{"FirstBurstLength", "65536"},
			{"MaxRecvDataSegmentLength", "65536"},
			{"DefaultTime2Wait", "2"},
			{"DefaultTime2Retain", "0"},
			{"MaxOutstandingR2T", "1"},
			{"IFMarker", "No"},
			{"OFMarker", "No"},
			{"DataPDUInOrder", "Yes"},
			{"DataSequenceInOrder", "Yes"},
		}),
	}
	pairs := util.ParseKVText(cmd.RawData)
	if initiatorName, ok := pairs["InitiatorName"]; ok {
		conn.initiator = initiatorName
	}
	if alias, ok := pairs["InitiatorAlias"]; ok {
		conn.initiatorAlias = alias
	}
	targetName := pairs["TargetName"]
	if sessType, ok := pairs["SessionType"]; ok {
		if sessType == "Normal" {
			conn.sessionType = SESSION_NORMAL
		} else {
			conn.sessionType = SESSION_DISCOVERY
		}
	}
	if conn.sessionType == SESSION_DISCOVERY {
		conn.tid = 0xffff
	} else {
		for _, t := range s.iSCSITargets {
			if t.SCSITarget.Name == targetName {
				target = t
				break
			}
		}
		if target == nil {
			conn.state = CONN_STATE_EXIT
			return fmt.Errorf("No target found with name(%s)", targetName)
		}

		TPGT, err = target.FindTPG(conn.conn.LocalAddr().String())
		if err != nil {
			conn.state = CONN_STATE_EXIT
			return err
		}
		conn.tpgt = TPGT
		conn.tid = target.TID

	}
	switch conn.state {
	case CONN_STATE_FREE:
		conn.state = CONN_STATE_SECURITY
	case CONN_STATE_SECURITY:
	}

	conn.state = CONN_STATE_LOGIN_FULL
	conn.expCmdSN = cmd.CmdSN
	conn.statSN += 1
	conn.maxBurstLength = 262144
	conn.maxRecvDataSegmentLength = 65536
	conn.maxSeqCount = conn.maxBurstLength / conn.maxRecvDataSegmentLength

	switch conn.sessionType {
	case SESSION_NORMAL:
		if conn.session == nil {
			// create a new session
			sess, err := s.NewISCSISession(conn)
			if err != nil {
				glog.Error(err)
				return err
			}
			conn.session = sess
		}
	case SESSION_DISCOVERY:

	}

	return nil
}

func iscsiExecLogout(conn *iscsiConnection) error {
	cmd := conn.req
	conn.resp = &ISCSICommand{
		OpCode:  OpLogoutResp,
		StatSN:  cmd.ExpStatSN,
		TaskTag: cmd.TaskTag,
	}
	if conn.session == nil {
		conn.resp.ExpCmdSN = cmd.CmdSN
		conn.resp.MaxCmdSN = cmd.CmdSN
	} else {
		conn.resp.ExpCmdSN = conn.session.ExpCmdSN
		conn.resp.MaxCmdSN = conn.session.ExpCmdSN + 10
	}
	return nil
}

func (s *ISCSITargetService) iscsiExecText(conn *iscsiConnection) error {
	var result = []util.KeyValue{}
	cmd := conn.req
	keys := util.ParseKVText(cmd.RawData)
	if st, ok := keys["SendTargets"]; ok {
		if st == "All" {
			for name, tgt := range s.iSCSITargets {
				glog.Debugf("iscsi target: %v", name)
				//glog.Debugf("iscsi target portals: %v", tgt.Portals)

				result = append(result, util.KeyValue{"TargetName", name})
				for _, tpgt := range tgt.TPGTs {
					for portal := range tpgt.Portals {
						targetPort := fmt.Sprintf("%s,%d", portal, tpgt.TPGT)
						result = append(result, util.KeyValue{"TargetAddress", targetPort})
					}
				}
			}
		}
	}

	conn.resp = &ISCSICommand{
		OpCode:   OpTextResp,
		Final:    true,
		NSG:      FullFeaturePhase,
		StatSN:   cmd.ExpStatSN,
		TaskTag:  cmd.TaskTag,
		ExpCmdSN: cmd.CmdSN,
		MaxCmdSN: cmd.CmdSN,
	}
	conn.resp.RawData = util.MarshalKVText(result)
	return nil
}

func iscsiExecNoopOut(conn *iscsiConnection) error {
	cmd := conn.req
	conn.resp = &ISCSICommand{
		OpCode:   OpNoopIn,
		Final:    true,
		NSG:      FullFeaturePhase,
		StatSN:   cmd.ExpStatSN,
		TaskTag:  cmd.TaskTag,
		ExpCmdSN: cmd.CmdSN + 1,
		MaxCmdSN: cmd.CmdSN + 10,
	}
	return nil
}

func iscsiExecTMFunction(conn *iscsiConnection) error {
	cmd := conn.req
	conn.resp = &ISCSICommand{
		OpCode:   OpSCSITaskResp,
		Final:    true,
		NSG:      FullFeaturePhase,
		StatSN:   cmd.ExpStatSN,
		TaskTag:  cmd.TaskTag,
		ExpCmdSN: cmd.CmdSN + 1,
		MaxCmdSN: cmd.CmdSN + 10,
	}
	return nil
}

func iscsiExecReject(conn *iscsiConnection) error {
	conn.resp = &ISCSICommand{
		OpCode: OpReject,
	}
	return nil
}

func iscsiExecR2T(conn *iscsiConnection) error {
	conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: &api.SCSICommand{}}
	conn.txIOState = IOSTATE_TX_BHS
	conn.statSN += 1
	task := conn.rxTask
	resp := &ISCSICommand{
		OpCode:        OpReady,
		Immediate:     true,
		Final:         true,
		StatSN:        conn.req.ExpStatSN,
		TaskTag:       conn.req.TaskTag,
		ExpCmdSN:      conn.session.ExpCmdSN,
		MaxCmdSN:      conn.session.ExpCmdSN + 10,
		R2TSN:         task.r2tSN,
		BufferOffset:  uint32(task.offset),
		DesiredLength: uint32(task.r2tCount),
	}
	if val := sessionKeys[ISCSI_PARAM_MAX_BURST].def; task.r2tCount > int(val) {
		resp.DesiredLength = uint32(val)
	}
	conn.resp = resp
	return nil
}

type Msg struct {
	ConnNum     int
	OpCode      int
	BlockCount  int64
	ElapsedTime time.Duration
}

func (s *ISCSITargetService) StatsFeed() {
	var (
		//msg       Msg
		//iocounter int64
		TotalIOPSPS int64
		ReadIOPS    int64
		WriteIOPS   int64
		//TotalBlockCount int64

		TotalReadTimePS     time.Duration
		ReadsPS             int64
		ReadLatency         int64
		ReadThroughput      int64
		TotalReadBlockCount int64
		AvgReadBlockSize    int64

		TotalWriteTimePS     time.Duration
		WritesPS             int64
		WriteLatency         int64
		WriteThroughput      int64
		TotalWriteBlockCount int64
		AvgWriteBlockSize    int64
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			TotalIOPSPS = ReadsPS + WritesPS
			if ReadsPS != 0 {
				ReadIOPS = ReadsPS
				ReadLatency = int64(TotalReadTimePS) / ReadsPS
				AvgReadBlockSize = TotalReadBlockCount / ReadsPS
				ReadThroughput = AvgReadBlockSize * ReadsPS
			} else {
				ReadIOPS = 0
				ReadLatency = 0
				AvgReadBlockSize = 0
				ReadThroughput = 0
			}
			if WritesPS != 0 {
				WriteIOPS = WritesPS
				WriteLatency = int64(TotalWriteTimePS) / WritesPS
				AvgWriteBlockSize = TotalWriteBlockCount / WritesPS
				WriteThroughput = AvgWriteBlockSize * WritesPS
			} else {
				WriteIOPS = 0
				WriteLatency = 0
				AvgWriteBlockSize = 0
				WriteThroughput = 0
			}
			ReadsPS = 0
			TotalReadTimePS = 0
			WritesPS = 0
			TotalWriteTimePS = 0
			fmt.Println("IOPS =", TotalIOPSPS, ",RL =", ReadLatency, ",WL =", WriteLatency, ",AvgRD =", AvgReadBlockSize, ",AvgWR =", AvgWriteBlockSize, ",RDT =", ReadThroughput, ",WRT =", WriteThroughput)
		case <-s.stopFeed:
			return
		case msg := <-s.txIO:
			switch api.SCSICommandType(msg.OpCode) {
			case api.READ_6, api.READ_10, api.READ_12, api.READ_16:
				ReadsPS += 1
				TotalReadTimePS += msg.ElapsedTime
				TotalReadBlockCount += msg.BlockCount
				break
			case api.WRITE_6, api.WRITE_10, api.WRITE_12, api.WRITE_16:
				WritesPS += 1
				TotalWriteTimePS += msg.ElapsedTime
				TotalWriteBlockCount += msg.BlockCount
				break
			}
		case <-s.statsEnq:
			s.statsRes <- port.Stats{
				ReadIOPS:         ReadIOPS,
				ReadLatency:      ReadLatency,
				AvgReadBlockSize: AvgReadBlockSize,
				ReadThroughput:   ReadThroughput,

				WriteIOPS:         WriteIOPS,
				WriteLatency:      WriteLatency,
				AvgWriteBlockSize: AvgWriteBlockSize,
				WriteThroughput:   WriteThroughput,
			}

		}
	}
}

func (s *ISCSITargetService) txHandler(conn *iscsiConnection) {
	var (
		hdigest uint   = 0
		ddigest uint   = 0
		offset  uint32 = 0
		final   bool   = false
		count   uint32 = 0
		msg     Msg
	)
	if conn.state == CONN_STATE_SCSI {
		hdigest = conn.sessionParam[ISCSI_PARAM_HDRDGST_EN].Value & DIGEST_CRC32C
		ddigest = conn.sessionParam[ISCSI_PARAM_DATADGST_EN].Value & DIGEST_CRC32C
	}
	if conn.state == CONN_STATE_SCSI && conn.txTask == nil {
		err := s.scsiCommandHandler(conn)
		if err != nil {
			glog.Error(err)
			return
		}
	}
	resp := conn.resp
	segmentLen := conn.maxRecvDataSegmentLength
	transferLen := len(resp.RawData)
	resp.DataSN = 0
	maxCount := conn.maxSeqCount
	if resp.OpCode == OpSCSIResp || resp.OpCode == OpSCSIIn {
		msg.ConnNum = conn.ConnNum
		msg.OpCode = int(resp.SCSIOpCode)
		msg.ElapsedTime = time.Since(resp.StartTime)
		msg.BlockCount = int64(resp.ExpectedDataLen)
		s.txIO <- msg
	}
	/* send data splitted by segmentLen */
SendRemainingData:
	if resp.OpCode == OpSCSIIn {
		resp.BufferOffset = offset
		if int(offset+segmentLen) < transferLen {
			count += 1
			if count < maxCount {
				resp.FinalInSeq = false
				resp.Final = false
			} else {
				count = 0
				resp.FinalInSeq = true
				resp.Final = false
			}
			offset = offset + segmentLen
			resp.DataLen = int(segmentLen)
		} else {
			resp.FinalInSeq = true
			resp.Final = true
			resp.DataLen = transferLen - int(offset)
		}
	}
	for {
		switch conn.txIOState {
		case IOSTATE_TX_BHS:
			glog.Debugf("ready to write response")
			glog.Debugf("%s", conn.resp.String())
			glog.Debugf("length of RawData is %d", len(resp.RawData))
			glog.Debugf("length of resp is %d", len(resp.Bytes()))
			if l, err := conn.write(resp.Bytes()); err != nil {
				glog.Error(err)
				return
			} else {
				conn.txIOState = IOSTATE_TX_INIT_AHS
				glog.Debugf("success to write %d length", l)
			}
		case IOSTATE_TX_INIT_AHS:
			if hdigest != 0 {
				conn.txIOState = IOSTATE_TX_INIT_HDIGEST
			} else {
				conn.txIOState = IOSTATE_TX_INIT_DATA
			}
			if conn.txIOState != IOSTATE_TX_AHS {
				final = true
			}
		case IOSTATE_TX_AHS:
		case IOSTATE_TX_INIT_DATA:
			final = true
		case IOSTATE_TX_DATA:
			if ddigest != 0 {
				conn.txIOState = IOSTATE_TX_INIT_DDIGEST
			}
		default:
			glog.Errorf("error %d %d\n", conn.state, conn.txIOState)
			return
		}

		if final {
			if resp.OpCode == OpSCSIIn && conn.resp.Final != true {
				resp.DataSN++
				conn.txIOState = IOSTATE_TX_BHS
				goto SendRemainingData
			} else {
				break
			}
		}
	}

	glog.Debugf("connection state: %d", conn.state)
	switch conn.state {
	case CONN_STATE_CLOSE, CONN_STATE_EXIT:
		glog.Warningf("set connection to close")
		conn.state = CONN_STATE_CLOSE
	case CONN_STATE_SECURITY_LOGIN:
		conn.state = CONN_STATE_LOGIN
		glog.Debugf("CONN_STATE_LOGIN")
	case CONN_STATE_SECURITY_FULL, CONN_STATE_LOGIN_FULL:
		if conn.sessionType == SESSION_NORMAL {
			conn.state = CONN_STATE_KERNEL
			glog.Debugf("CONN_STATE_KERNEL")
			conn.state = CONN_STATE_SCSI
			glog.Debugf("CONN_STATE_SCSI")
		} else {
			conn.state = CONN_STATE_FULL
			glog.Debugf("CONN_STATE_FULL")
		}
		conn.rxIOState = IOSTATE_RX_BHS
		s.handler(DATAIN, conn)
	case CONN_STATE_SCSI:
		conn.txTask = nil
	default:
		glog.Warningf("unexpected connection state: %d", conn.state)
		conn.rxIOState = IOSTATE_RX_BHS
		s.handler(DATAIN, conn)
	}
	glog.Debugf("%d", conn.state)
}

func (s *ISCSITargetService) scsiCommandHandler(conn *iscsiConnection) (err error) {
	req := conn.req
	switch req.OpCode {
	case OpSCSICmd:
		glog.Debugf("SCSI Command processing...")
		if _, ok := s.SCSIIOCount[(int)(req.CDB[0])]; ok != false {
			s.SCSIIOCount[(int)(req.CDB[0])] += 1
		} else {
			s.SCSIIOCount[(int)(req.CDB[0])] = 1
		}
		scmd := &api.SCSICommand{}
		task := &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: scmd}
		task.scmd.OpCode = conn.req.SCSIOpCode
		if req.Write {
			task.offset = req.DataLen
			task.expectedDataLength = int64(req.ExpectedDataLen)
			task.r2tCount = int(req.ExpectedDataLen) - req.DataLen
			if !req.Final {
				task.unsolCount = 1
			}
			glog.Debugf("SCSI write, R2T count: %d, unsol Count: %d, offset: %d", task.r2tCount, task.unsolCount, task.offset)

			if task.scmd.OutSDBBuffer.Buffer == nil {
				//task.scmd.OutSDBBuffer.Buffer = bytes.NewBuffer([]byte{})
				task.scmd.OutSDBBuffer.Buffer = util.WPool.Get().(*bytes.Buffer)
				task.scmd.OutSDBBuffer.Buffer.Grow(int(req.ExpectedDataLen))
			}
			task.scmd.OutSDBBuffer.Buffer.Write(conn.req.RawData)
			if task.r2tCount > 0 {
				// prepare to receive more data
				task.state = taskPending
				conn.session.PendingTasks.Push(task)
				conn.rxTask = task
				conn.session.ExpCmdSN += 1
				iscsiExecR2T(conn)
				break
			}
		}
		task.offset = 0
		conn.rxTask = task
		if err = s.iscsiTaskQueueHandler(task); err != nil {
			return
		} else {
			conn.rxTask = nil
			conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: &api.SCSICommand{}}
			conn.txIOState = IOSTATE_TX_BHS
			conn.statSN += 1
			resp := &ISCSICommand{
				StartTime:       req.StartTime,
				ExpectedDataLen: req.ExpectedDataLen,
				Immediate:       true,
				Final:           true,
				StatSN:          req.ExpStatSN,
				TaskTag:         req.TaskTag,
				ExpCmdSN:        conn.session.ExpCmdSN,
				MaxCmdSN:        conn.session.ExpCmdSN + 10,
				Status:          scmd.Result,
				SCSIResponse:    0x00,
				HasStatus:       true,
				SenseLen:        scmd.SenseLength,
			}
			switch scmd.Direction {
			case api.SCSIDataRead:
				resp.OpCode = OpSCSIIn
				resp.SCSIOpCode = conn.req.SCSIOpCode
				if scmd.InSDBBuffer.Buffer != nil {
					buf := scmd.InSDBBuffer.Buffer.Bytes()
					resp.RawData = buf
				} else {
					resp.RawData = []byte{}
				}
			case api.SCSIDataWrite:
				resp.OpCode = OpSCSIResp
				if scmd.InSDBBuffer.Buffer != nil {
					buf := scmd.InSDBBuffer.Buffer.Bytes()
					resp.RawData = buf
				} else {
					resp.RawData = []byte{}
				}
			case api.SCSIDataBidirection:
			case api.SCSIDataNone:
				resp.OpCode = OpSCSIResp
			}
			if scmd.Result != 0 && scmd.SenseBuffer != nil {
				resp.OpCode = OpSCSIResp
				resp.RawData = scmd.SenseBuffer.Bytes()
			}
			conn.resp = resp
		}
	case OpSCSITaskReq:
		// task management function
		conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, state: taskPending}
		conn.txIOState = IOSTATE_TX_BHS
		iscsiExecTMFunction(conn)
	case OpSCSIOut:
		glog.Debugf("iSCSI Data-out processing...")
		var task *iscsiTask
		for _, t := range conn.session.PendingTasks {
			if t.tag == conn.req.TaskTag {
				task = t
			}
		}
		if task == nil {
			err = fmt.Errorf("Cannot find iSCSI task with tag[%v]", conn.req.TaskTag)
			glog.Error(err)
			return
		}
		task.offset = task.offset + conn.req.DataLen
		task.r2tCount = task.r2tCount - conn.req.DataLen
		task.scmd.OutSDBBuffer.Buffer.Write(conn.req.RawData)
		glog.Debugf("Final: %v", conn.req.Final)
		glog.Debugf("r2tCount: %v", task.r2tCount)
		if !conn.req.Final {
			glog.Debugf("Not ready to exec the task")
			conn.rxIOState = IOSTATE_RX_BHS
			s.handler(DATAIN, conn)
			return nil
		} else if task.r2tCount > 0 {
			// prepare to receive more data
			task.r2tSN += 1
			conn.rxTask = task
			iscsiExecR2T(conn)
			break
		}
		task.offset = 0
		glog.Debugf("Process the Data-out package")
		conn.rxTask = task
		if err = s.iscsiExecTask(task); err != nil {
			return
		} else {
			conn.rxTask = nil
			conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: &api.SCSICommand{}, state: taskSCSI}
			conn.txIOState = IOSTATE_TX_BHS
			conn.statSN += 1
			resp := &ISCSICommand{
				StartTime:       req.StartTime,
				OpCode:          OpSCSIResp,
				ExpectedDataLen: uint32(task.expectedDataLength),
				Immediate:       true,
				Final:           true,
				StatSN:          req.ExpStatSN,
				TaskTag:         req.TaskTag,
				ExpCmdSN:        conn.session.ExpCmdSN,
				MaxCmdSN:        conn.session.ExpCmdSN + 10,
				Status:          task.scmd.Result,
				SCSIOpCode:      task.scmd.OpCode,
				SCSIResponse:    0x00,
				HasStatus:       true,
				SenseLen:        task.scmd.SenseLength,
			}
			if task.scmd.Result != 0 && task.scmd.SenseBuffer != nil {
				resp.RawData = task.scmd.SenseBuffer.Bytes()
			}
			conn.resp = resp
		}
	case OpNoopOut:
		conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag}
		conn.txIOState = IOSTATE_TX_BHS
		iscsiExecNoopOut(conn)
	case OpLogoutReq:
		conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag}
		conn.txIOState = IOSTATE_TX_BHS
		iscsiExecLogout(conn)
	case OpTextReq, OpSNACKReq:
		err = fmt.Errorf("Cannot handle yet %s", opCodeMap[conn.req.OpCode])
		glog.Error(err)
		return
	default:
		err = fmt.Errorf("Unknown op %s", opCodeMap[conn.req.OpCode])
		glog.Error(err)
		return
	}
	conn.rxIOState = IOSTATE_RX_BHS
	s.handler(DATAIN|DATAOUT, conn)
	return nil
}

func (s *ISCSITargetService) iscsiTaskQueueHandler(task *iscsiTask) error {
	conn := task.conn
	sess := conn.session
	cmd := task.cmd
	if cmd.Immediate {
		return s.iscsiExecTask(task)
	}
	cmdsn := cmd.CmdSN
	glog.Debugf("CmdSN of command is %d, ExpCmdSN of session is %d", cmdsn, sess.ExpCmdSN)
	if cmdsn == sess.ExpCmdSN {
	retry:
		cmdsn += 1
		sess.ExpCmdSN = cmdsn
		glog.Debugf("session's ExpCmdSN is %d", cmdsn)

		glog.Debugf("process task(%d)", task.cmd.CmdSN)
		if err := s.iscsiExecTask(task); err != nil {
			glog.Error(err)
		}
		if len(sess.PendingTasks) == 0 {
			return nil
		}
		task = sess.PendingTasks.Pop().(*iscsiTask)
		cmd = task.cmd
		if cmd.CmdSN != cmdsn {
			sess.PendingTasks.Push(task)
			return nil
		}
		task.state = taskSCSI
		goto retry
	} else {
		if cmd.CmdSN < sess.ExpCmdSN {
			err := fmt.Errorf("unexpected cmd serial number: (%d, %d)", cmd.CmdSN, sess.ExpCmdSN)
			glog.Error(err)
			return err
		}
		glog.Debugf("add task(%d) into task queue", task.cmd.CmdSN)
		// add this connection into queue and set this task as pending task
		task.state = taskPending
		sess.PendingTasks.Push(task)
		return fmt.Errorf("pending")
	}

	return nil
}

func (s *ISCSITargetService) iscsiExecTask(task *iscsiTask) error {
	cmd := task.cmd
	switch cmd.OpCode {
	case OpSCSICmd, OpSCSIOut:
		if cmd.Read {
			if cmd.Write {
				task.scmd.Direction = api.SCSIDataBidirection
			} else {
				task.scmd.Direction = api.SCSIDataRead
			}
		} else {
			if cmd.Write {
				task.scmd.Direction = api.SCSIDataWrite
			}
		}
		task.scmd.CommandITNID = task.conn.session.Tsih
		task.scmd.SCB = bytes.NewBuffer(cmd.CDB)
		task.scmd.SCBLength = len(cmd.CDB)
		task.scmd.Lun = cmd.LUN
		task.scmd.Tag = uint64(cmd.TaskTag)
		task.scmd.RelTargetPortID = task.conn.tpgt
		task.state = taskSCSI
		if task.scmd.OutSDBBuffer.Buffer == nil {
			task.scmd.OutSDBBuffer.Buffer = bytes.NewBuffer(cmd.RawData)
		}
		// add scsi target process queue
		err := s.SCSI.AddCommandQueue(task.conn.session.Target.SCSITarget.TID, task.scmd)
		if err != nil {
			task.state = 0
		}
		return err
	case OpLogoutReq:

	case OpNoopOut:
		// just do it in iscsi layer
	}
	return nil
}
