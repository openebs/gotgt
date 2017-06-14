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

package port

import (
	"fmt"

	"github.com/openebs/gotgt/pkg/config"
	"github.com/openebs/gotgt/pkg/scsi"
)

type SCSITargetService interface {
	Run() error
	Stop() error
	NewTarget(string, *config.Config) (SCSITargetDriver, error)
	Stats() Stats
	Resize(uint64) error
	SetClusterIP(string) error
}

type Stats struct {
	RevisionCounter int64
	ReplicaCounter  int64
	SCSIIOCount     map[int]int64

	ReadIOPS         int64
	ReadThroughput   int64
	ReadLatency      int64
	AvgReadBlockSize int64

	WriteIOPS         int64
	WriteThroughput   int64
	WriteLatency      int64
	AvgWriteBlockSize int64
}

type TargetServiceFunc func(*scsi.SCSITargetService) (SCSITargetService, error)

var registeredPlugins = map[string](TargetServiceFunc){}

func RegisterTargetService(name string, f TargetServiceFunc) {
	registeredPlugins[name] = f
}

func NewTargetService(targetDriverName string, s *scsi.SCSITargetService) (SCSITargetService, error) {
	if targetDriverName == "" {
		return nil, nil
	}
	targetInitFunc, ok := registeredPlugins[targetDriverName]
	if !ok {
		return nil, fmt.Errorf("SCSI target driver %s is not found.", targetDriverName)
	}
	return targetInitFunc(s)
}
