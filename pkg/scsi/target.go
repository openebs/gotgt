/*
Copyright 2015 The GoStor Authors All rights reserved.

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

package scsi

import (
	"fmt"
	"unsafe"

	glog "github.com/Sirupsen/logrus"
	"github.com/gostor/gotgt/pkg/api"
)

func (s *SCSITargetService) NewSCSITarget(tid int, driverName, name string) (*api.SCSITarget, error) {
	// verify the target ID

	// verify the target's Name

	// verify the low level driver
	var target = &api.SCSITarget{
		Name:             name,
		TID:              tid,
		TargetPortGroups: []*api.TargetPortGroup{},
	}
	tpg := &api.TargetPortGroup{0, []*api.SCSITargetPort{}}
	s.Targets = append(s.Targets, target)
	target.Devices = GetTargetLUNMap(target.Name)
	target.LUN0 = NewLUN0()
	target.TargetPortGroups = append(target.TargetPortGroups, tpg)
	return target, nil
}

func FindTargetGroup(target *api.SCSITarget, relPortID uint16) uint16 {
	for _, tpg := range target.TargetPortGroups {
		for _, port := range tpg.TargetPortGroup {
			if port.RelativeTargetPortID == relPortID {
				return tpg.GroupID
			}
		}
	}
	return 0
}

func FindTargetPort(target *api.SCSITarget, relPortID uint16) *api.SCSITargetPort {
	for _, tpg := range target.TargetPortGroups {
		for _, port := range tpg.TargetPortGroup {
			if port.RelativeTargetPortID == relPortID {
				return port
			}
		}
	}
	return nil
}

func deviceReserve(cmd *api.SCSICommand) error {
	var lu *api.SCSILu
	lun := *(*uint64)(unsafe.Pointer(&cmd.Lun))

	for tgtLUN, lunDev := range cmd.Target.Devices {
		if tgtLUN == lun {
			lu = lunDev
			break
		}
	}
	if lu == nil {
		glog.Errorf("invalid target and lun %d %s", cmd.Target.TID, lun)
		return nil
	}

	if lu.ReserveID != 0 && lu.ReserveID != cmd.CommandITNID {
		glog.Errorf("already reserved %d, %d", lu.ReserveID, cmd.CommandITNID)
		return fmt.Errorf("already reserved")
	}
	lu.ReserveID = cmd.CommandITNID
	return nil
}

func deviceRelease(tid int, itn, lun uint64, force bool) error {
	// TODO
	return nil
}
