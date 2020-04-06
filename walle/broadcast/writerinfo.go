package broadcast

import (
	"bytes"
	"context"
	"sort"
	"sync"

	walle_pb "github.com/zviadm/walle/proto/walle"
	"github.com/zviadm/walle/proto/walleapi"
)

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
	_, err := Call(ctx, cli, topology.ServerIds, 0, 0,
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
			if respMax == nil || bytes.Compare(resp.WriterId, respMax.WriterId) > 0 {
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
	// Sort responses by (writerId, remainingLeaseMs) and choose one that majority is
	// greather than or equal to.
	sort.Slice(remainingMs, func(i, j int) bool { return remainingMs[i] < remainingMs[j] })
	sort.Slice(streamVersions, func(i, j int) bool { return streamVersions[i] > streamVersions[j] })
	respMax.RemainingLeaseMs = remainingMs[len(topology.ServerIds)/2]
	respMax.StreamVersion = streamVersions[len(topology.ServerIds)/2]
	return respMax, nil
}
