package iscsit

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/openebs/gotgt/pkg/api"
	"github.com/openebs/gotgt/pkg/config"
	"github.com/openebs/gotgt/pkg/scsi"
	_ "github.com/openebs/gotgt/pkg/scsi/backingstore"
	"github.com/sirupsen/logrus"
)

type goTgt struct {
	Volume     string
	Size       int64
	SectorSize int

	isUp bool
	rw   api.IOs

	tgtName      string
	lhbsName     string
	cfg          *config.Config
	targetDriver scsi.SCSITargetDriver
}

type FakeRW struct {
}

func (c *FakeRW) ReadAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}

func (c *FakeRW) WriteAt(buf []byte, offset int64) (int, error) {
	return 0, nil
}

func (c *FakeRW) Sync() (int, error) {
	return 0, nil
}

func TestOne(t *testing.T) {

	name := "volume-name"
	rw := &FakeRW{}
	tgt := &goTgt{
		Volume:     name,
		Size:       1074790400,
		SectorSize: 4096,
		rw:         rw,
		tgtName:    "iqn.2016-09.com.openebs.jiva:" + name,
		lhbsName:   "RemBs:" + name,
	}
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	var ip string
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip = ipv4.String()
			break
			//fmt.Println("IPv4: ", ipv4)
		}
	}
	tgt.cfg = &config.Config{
		Storages: []config.BackendStorage{
			config.BackendStorage{
				DeviceID: 1000,
				Path:     tgt.lhbsName,
				Online:   true,
			},
		},
		ISCSIPortals: []config.ISCSIPortalInfo{
			config.ISCSIPortalInfo{
				ID:     0,
				Portal: ip + ":3260",
			},
		},
		ISCSITargets: map[string]config.ISCSITarget{
			tgt.tgtName: config.ISCSITarget{
				TPGTs: map[string][]uint64{
					"1": []uint64{0},
				},
				LUNs: map[string]uint64{
					"1": uint64(1000),
				},
			},
		},
	}

	scsiTarget := scsi.NewSCSITargetService()
	var err error
	tgt.targetDriver, err = scsi.NewTargetDriver("iscsi", scsiTarget)
	if err != nil {
		logrus.Errorf("iscsi target driver error")
		return
	}
	scsi.InitSCSILUMapEx(tgt.tgtName, tgt.Volume, 1, 1, uint64(tgt.Size), uint64(tgt.SectorSize), tgt.rw)
	tgt.targetDriver.NewTarget(tgt.tgtName, tgt.cfg)
	go tgt.targetDriver.Run()

	time.Sleep(10 * time.Second)

	logrus.Infof("stopping target %v ...", tgt.tgtName)
	tgt.targetDriver.Stop()
	logrus.Infof("target %v stopped", tgt.tgtName)
}

func TestTwo(t *testing.T) {
	name := "volume-name"
	rw := &FakeRW{}
	tgt := &goTgt{
		Volume:     name,
		Size:       1074790400,
		SectorSize: 4096,
		rw:         rw,
		tgtName:    "iqn.2016-09.com.openebs.jiva:" + name,
		lhbsName:   "RemBs:" + name,
	}
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	var ip string
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip = ipv4.String()
			break
			//fmt.Println("IPv4: ", ipv4)
		}
	}
	tgt.cfg = &config.Config{
		Storages: []config.BackendStorage{
			config.BackendStorage{
				DeviceID: 1000,
				Path:     tgt.lhbsName,
				Online:   true,
			},
		},
		ISCSIPortals: []config.ISCSIPortalInfo{
			config.ISCSIPortalInfo{
				ID:     0,
				Portal: ip + ":3260",
			},
		},
		ISCSITargets: map[string]config.ISCSITarget{
			tgt.tgtName: config.ISCSITarget{
				TPGTs: map[string][]uint64{
					"1": []uint64{0},
				},
				LUNs: map[string]uint64{
					"1": uint64(1000),
				},
			},
		},
	}

	scsiTarget := scsi.NewSCSITargetService()

	var err error
	tgt.targetDriver, err = scsi.NewTargetDriver("iscsi", scsiTarget)
	if err != nil {
		panic(err)
	}
	scsi.InitSCSILUMapEx(tgt.tgtName, tgt.Volume, 1, 1, uint64(tgt.Size), uint64(tgt.SectorSize), tgt.rw)
	tgt.targetDriver.NewTarget(tgt.tgtName, tgt.cfg)

	logrus.Infof("test stopping target %v ...", tgt.tgtName)
	tgt.targetDriver.Stop()
	logrus.Infof("test target %v stopped", tgt.tgtName)

	go tgt.targetDriver.Run()

	time.Sleep(30 * time.Second)
	logrus.Infof("stopping target %v ...", tgt.tgtName)
	tgt.targetDriver.Stop()
	logrus.Infof("target %v stopped", tgt.tgtName)

}
