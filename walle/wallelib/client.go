package wallelib

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"time"

	walle_pb "github.com/zviadm/walle/proto/walle"
)

func ClaimWriter(
	ctx context.Context,
	c Client,
	streamURI string) (*Writer, error) {
	writerId := makeWriterId()
	resp, err := c.Preferred(streamURI).NewWriter(ctx, &walle_pb.NewWriterRequest{
		StreamUri: streamURI,
		WriterId:  writerId,
	})
	if err != nil {
		return nil, err
	}

	var entries map[string][]*walle_pb.Entry
	for {
		entries = make(map[string][]*walle_pb.Entry, len(resp.SuccessIds))
		for _, serverId := range resp.SuccessIds {
			r, err := c.ForServer(serverId).LastEntry(ctx, &walle_pb.LastEntryRequest{
				TargetServerId:     serverId,
				StreamUri:          streamURI,
				IncludeUncommitted: true,
			})
			if err != nil {
				return nil, err
			}
			entries[serverId] = r.Entries
		}
		committed, err := commitMaxEntry(ctx, c, streamURI, entries)
		if err != nil {
			return nil, err
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
	_, err = c.ForServer(maxWriterServerId).PutEntry(ctx, &walle_pb.PutEntryRequest{
		TargetServerId: maxWriterServerId,
		StreamUri:      streamURI,
		Entry:          maxEntry,
	})
	if err != nil {
		return nil, err
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
			_, err = c.ForServer(serverId).PutEntry(ctx, &walle_pb.PutEntryRequest{
				TargetServerId: serverId,
				StreamUri:      streamURI,
				Entry:          entry,
			})
			if err != nil {
				return nil, err
			}
		}
	}
	for serverId, _ := range entries {
		_, err = c.ForServer(serverId).PutEntry(ctx, &walle_pb.PutEntryRequest{
			TargetServerId:    serverId,
			StreamUri:         streamURI,
			Entry:             maxEntry,
			CommittedEntryId:  maxEntry.EntryId,
			CommittedEntryMd5: maxEntry.ChecksumMd5,
		})
		if err != nil {
			return nil, err
		}
	}
	w := newWriter(c, streamURI, writerId, maxEntry)
	return w, nil
}

func commitMaxEntry(
	ctx context.Context,
	c Client,
	streamURI string,
	entries map[string][]*walle_pb.Entry) (bool, error) {
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
			_, err := c.ForServer(serverId).PutEntry(ctx, &walle_pb.PutEntryRequest{
				TargetServerId:    serverId,
				StreamUri:         streamURI,
				Entry:             maxEntry,
				CommittedEntryId:  maxEntry.EntryId,
				CommittedEntryMd5: maxEntry.ChecksumMd5,
			})
			if err != nil {
				return false, err
			}
		}
	}
	return committed, nil
}

func makeWriterId() string {
	writerId := make([]byte, 16)
	binary.BigEndian.PutUint64(writerId[0:8], uint64(time.Now().UnixNano()))
	rand.Read(writerId[8:15])
	return string(writerId)
}
