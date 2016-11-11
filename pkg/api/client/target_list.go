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
	"net/url"

	"github.com/openebs/gotgt/pkg/api"
	"golang.org/x/net/context"
)

// TargetCreate creates a target in the SCSI Target.
func (cli *Client) TargetList(ctx context.Context, options api.TargetListOptions) ([]api.SCSITarget, error) {
	var targets []api.SCSITarget
	var query = url.Values{}
	if options.Name != "" {
		query.Set("name", options.Name)
	}
	resp, err := cli.get(ctx, "/target/list", query, nil)
	if err != nil {
		return targets, err
	}
	err = json.NewDecoder(resp.body).Decode(&targets)
	ensureReaderClosed(resp)
	return targets, err
}
