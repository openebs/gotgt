/*
Copyright 2016 openebs authors All rights reserved.

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

package backingstore

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/glog"
	"github.com/gostor/gotgt/pkg/api"
	"github.com/gostor/gotgt/pkg/scsi"
)

func init() {
	scsi.RegisterBackingStore("RemBs", newRemBs)
}

type RemBackingStore struct {
	scsi.BaseBackingStore
	RemBs api.ReaderWriterAt
}

func newRemBs() (api.BackingStore, error) {
	return &RemBackingStore{
		BaseBackingStore: scsi.BaseBackingStore{
			Name:            "RemBs",
			OflagsSupported: 0,
		},
	}, nil
}

func (bs *RemBackingStore) Open(dev *api.SCSILu, path string) error {
	bs.DataSize = uint64(dev.Size)
	bs.RemBs = scsi.GetTargetBSMap(path)
	return nil
}

func (bs *RemBackingStore) Close(dev *api.SCSILu) error {
	/* TODO return bs.File.Close()*/
	return nil
}

func (bs *RemBackingStore) Init(dev *api.SCSILu, Opts string) error {
	return nil
}

func (bs *RemBackingStore) Exit(dev *api.SCSILu) error {
	return nil
}

func (bs *RemBackingStore) Size(dev *api.SCSILu) uint64 {
	return bs.DataSize
}
func (bs *RemBackingStore) CommandSubmit(cmd *api.SCSICommand) (err error) {
	var (
		scb             = cmd.SCB.Bytes()
		offset          = cmd.Offset
		opcode          = api.SCSICommandType(scb[0])
		lu              = cmd.Device
		key             = scsi.ILLEGAL_REQUEST
		asc             = scsi.ASC_INVALID_FIELD_IN_CDB
		wbuf     []byte = []byte{}
		rbuf            = make([]byte, cmd.TL)
		length   int
		doVerify bool = false
		doWrite  bool = false
	)
	switch opcode {
	case api.ORWRITE_16:
		tmpbuf := []byte{}
		length, err = bs.RemBs.ReadAt(tmpbuf, int64(offset))
		if length != len(tmpbuf) {
			key = scsi.MEDIUM_ERROR
			asc = scsi.ASC_READ_ERROR
			break
		}
		cmd.InSDBBuffer.Buffer = bytes.NewBuffer(tmpbuf)

		wbuf = cmd.OutSDBBuffer.Buffer.Bytes()
		doWrite = true
		goto write
	case api.COMPARE_AND_WRITE:
		// TODO
		doWrite = true
		goto write
	case api.SYNCHRONIZE_CACHE, api.SYNCHRONIZE_CACHE_16:
		/*if err = util.Fdatasync(bs.File); err != nil {
			panic(err)
		}*/
		err = nil
		break
	case api.WRITE_VERIFY, api.WRITE_VERIFY_12, api.WRITE_VERIFY_16:
		doVerify = true
	case api.WRITE_6, api.WRITE_10, api.WRITE_12, api.WRITE_16:
		wbuf = cmd.OutSDBBuffer.Buffer.Bytes()
		doWrite = true
		goto write
	case api.WRITE_SAME, api.WRITE_SAME_16:
		// TODO
		break
	case api.READ_6, api.READ_10, api.READ_12, api.READ_16:
		length, err = bs.RemBs.ReadAt(rbuf, int64(offset))
		if err != nil && err != io.EOF {
			key = scsi.MEDIUM_ERROR
			asc = scsi.ASC_READ_ERROR
			break
		}
		for i := 0; i < int(cmd.TL)-length; i++ {
			rbuf = append(rbuf, 0)
		}

		/*if (opcode != api.READ_6) && (scb[1]&0x10 != 0) {
			util.Fadvise(bs.File, int64(offset), int64(length), util.POSIX_FADV_NOREUSE)
		}*/
		cmd.InSDBBuffer.Buffer = bytes.NewBuffer(rbuf)
	case api.PRE_FETCH_10, api.PRE_FETCH_16:
		/*err = util.Fadvise(bs.File, int64(offset), int64(cmd.TL), util.POSIX_FADV_WILLNEED)*/
		err = nil
		if err != nil {
			key = scsi.MEDIUM_ERROR
			asc = scsi.ASC_READ_ERROR
		}
	case api.VERIFY_10, api.VERIFY_12, api.VERIFY_16:
		doVerify = true
		goto verify
	case api.UNMAP:
		// TODO
	default:
		break
	}
write:
	if doWrite {
		// hack: wbuf = []byte("hello world!")
		length, err = bs.RemBs.WriteAt(wbuf, int64(offset))
		if err != nil || length != len(wbuf) {
			glog.Error(err)
			key = scsi.MEDIUM_ERROR
			asc = scsi.ASC_READ_ERROR
			goto sense
		}
		glog.V(2).Infof("write data at %d for length %d", offset, length)
		var pg *api.ModePage
		for _, p := range lu.ModePages {
			if p.Pcode == 0x08 && p.SubPcode == 0 {
				pg = &p
				break
			}
		}
		if pg == nil {
			key = scsi.ILLEGAL_REQUEST
			asc = scsi.ASC_INVALID_FIELD_IN_CDB
			goto sense
		}
		/*if ((opcode != api.WRITE_6) && (scb[1]&0x8 != 0)) || (pg.Data[0]&0x04 == 0) {
			if err = util.Fdatasync(bs.File); err != nil {
				key = scsi.MEDIUM_ERROR
				asc = scsi.ASC_READ_ERROR
				goto sense
			}
		}*/

		/*if (opcode != api.WRITE_6) && (scb[1]&0x10 != 0) {
			util.Fadvise(bs.File, int64(offset), int64(length), util.POSIX_FADV_NOREUSE)
		}*/
	}
verify:
	if doVerify {
		length, err = bs.RemBs.ReadAt(rbuf, int64(offset))
		if length != len(rbuf) {
			key = scsi.MEDIUM_ERROR
			asc = scsi.ASC_READ_ERROR
			goto sense
		}
		if !bytes.Equal(cmd.OutSDBBuffer.Buffer.Bytes(), rbuf) {
			err = fmt.Errorf("verify fail between out buffer and read buffer")
			key = scsi.MISCOMPARE
			asc = scsi.ASC_MISCOMPARE_DURING_VERIFY_OPERATION
			goto sense
		}
		/*if scb[1]&0x10 != 0 {
			util.Fadvise(bs.File, int64(offset), int64(length), util.POSIX_FADV_WILLNEED)
		}*/
	}
	glog.Infof("io done %s", string(scb))
sense:
	if err != nil {
		glog.Error(err)
		return err
	}
	_ = key
	_ = asc

	return nil
}
