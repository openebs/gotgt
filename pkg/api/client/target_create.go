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
package client

import (
	"encoding/json"

	"github.com/openebs/gotgt/pkg/api"
	"golang.org/x/net/context"
)

// TargetCreate creates a target in the SCSI Target.
func (cli *Client) TargetCreate(ctx context.Context, options api.TargetCreateRequest) (api.SCSITarget, error) {
	var target api.SCSITarget
	resp, err := cli.post(ctx, "/target/create", nil, options, nil)
	if err != nil {
		return target, err
	}
	err = json.NewDecoder(resp.body).Decode(&target)
	ensureReaderClosed(resp)
	return target, err
}
