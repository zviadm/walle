package broadcast

import (
	"context"
	"sort"
	"sync"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/walle/storage"
)

// WriterInfo broadcasts WriterInfo rpc to all servers and merges result into a
// single WriterInfoResponse based on majority of responses.
func WriterInfo(
	ctx context.Context,
	cli Client,
	fromServerId string,
	streamURI string,
	topology *walleapi.StreamTopology) (*walle_pb.WriterInfoResponse, error) {
	respMx := sync.Mutex{}
	var respMax *walle_pb.WriterInfoResponse
	var remainingMs []int64
	var streamVersions []int64

	waitLive := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		deadlineTimeout := deadline.Sub(time.Now()) * 4 / 5
		if deadlineTimeout < waitLive {
			waitLive = deadlineTimeout
		}
	}
	_, err := Call(ctx, cli, topology.ServerIds, waitLive, 0,
		func(c walle_pb.WalleClient, ctx context.Context, serverId string) error {
			resp, err := c.WriterInfo(ctx, &walle_pb.WriterInfoRequest{
				ServerId:      serverId,
				StreamUri:     streamURI,
				StreamVersion: topology.Version,
				FromServerId:  fromServerId,
			})
			if err != nil {
				return err
			}
			respMx.Lock()
			defer respMx.Unlock()
			if respMax == nil || storage.CmpWriterIds(resp.WriterId, respMax.WriterId) > 0 {
				respMax = resp
			}
			remainingMs = append(remainingMs, resp.RemainingLeaseMs)
			streamVersions = append(streamVersions, resp.StreamVersion)
			return nil
		})
	if err != nil {
		return nil, err
	}
	respMx.Lock()
	defer respMx.Unlock()
	// Find streamVersion that is smallest in majority. Smallest version in majority is considered as the
	// current version.
	sort.Slice(streamVersions, func(i, j int) bool { return streamVersions[i] > streamVersions[j] })
	// Find largest remainingMs in majority. Since broadcast call waits a bit to get responses from all
	// servers, it should be able to get responses from all live servers. Choosing largest, errs more
	// on the side of keeping current writer stable.
	sort.Slice(remainingMs, func(i, j int) bool { return remainingMs[i] < remainingMs[j] })
	respMax.StreamVersion = streamVersions[len(topology.ServerIds)/2]
	respMax.RemainingLeaseMs = remainingMs[len(topology.ServerIds)/2]
	return respMax, nil
}
