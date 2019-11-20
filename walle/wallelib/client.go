package wallelib

import (
	"bytes"
	"context"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

type client struct {
	c walle_pb.WalleClient
}

func (c *client) ClaimWriter(ctx context.Context, streamURI string) error {
	writerId := makeWriterId()
	resp, err := c.c.NewWriter(ctx, &walle_pb.NewWriterRequest{
		StreamUri: streamURI,
		WriterId:  writerId,
	})

	if err != nil {
		return err
	}
	var entries map[string][]*walle_pb.Entry
	for {
		entries = make(map[string][]*walle_pb.Entry, len(resp.SuccessIds))
		for _, serverId := range resp.SuccessIds {
			// TODO(zviad): use proper client, for serverId.
			r, err := c.c.LastEntry(ctx, &walle_pb.LastEntryRequest{
				TargetServerId:     serverId,
				StreamUri:          streamURI,
				IncludeUncommitted: true,
			})
			if err != nil {
				return err
			}
			entries[serverId] = r.Entries
		}
		committed, err := c.commitMaxEntry(ctx, entries)
		if err != nil {
			return err
		}
		if !committed {
			break // Nothing to commit, thus all servers are at the same committed entry.
		}
	}

	var maxWriterServerId string
	var maxEntry *walle_pb.Entry
	for serverId, es := range entries {
		e := es[len(es)-1]
		if maxEntry == nil || e.WriterId > maxEntry.WriterId ||
			(e.WriterId == maxEntry.WriterId && e.EntryId > maxEntry.EntryId) {
			maxWriterServerId = serverId
			maxEntry = e
		}
	}
	maxEntry.WriterId = writerId
	_, err = c.c.PutEntry(ctx, &walle_pb.PutEntryRequest{
		TargetServerId: maxWriterServerId,
		Entry:          maxEntry,
	})
	if err != nil {
		return err
	}
	maxEntries := entries[maxWriterServerId]
	for serverId, es := range entries {
		if serverId == maxWriterServerId {
			continue
		}
		startIdx := len(es)
		for idx, entry := range es {
			if bytes.Compare(entry.ChecksumMd5, maxEntries[idx].ChecksumMd5) != 0 {
				startIdx = idx
				break
			}
		}
		for idx := startIdx; idx < len(maxEntries); idx++ {
			entry := maxEntries[idx]
			entry.WriterId = writerId
			_, err = c.c.PutEntry(ctx, &walle_pb.PutEntryRequest{
				TargetServerId: serverId,
				Entry:          entry,
			})
			if err != nil {
				return err
			}
		}
	}
	for serverId, _ := range entries {
		_, err = c.c.PutEntry(ctx, &walle_pb.PutEntryRequest{
			TargetServerId:   serverId,
			Entry:            maxEntry,
			CommittedEntryId: maxEntry.EntryId,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) commitMaxEntry(
	ctx context.Context, entries map[string][]*walle_pb.Entry) (bool, error) {
	var maxEntry *walle_pb.Entry
	committed := false
	for _, es := range entries {
		entryId := es[0].EntryId
		if maxEntry == nil || entryId > maxEntry.EntryId {
			maxEntry = es[0]
		}
	}
	for serverId, es := range entries {
		if es[0].EntryId < maxEntry.EntryId {
			committed = true
			_, err := c.c.PutEntry(ctx, &walle_pb.PutEntryRequest{
				TargetServerId:   serverId,
				Entry:            maxEntry,
				CommittedEntryId: maxEntry.EntryId,
			})
			if err != nil {
				return false, err
			}
		}
	}
	return committed, nil
}

func makeWriterId() string {
	return ""
}
