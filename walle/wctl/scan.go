package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/zviadm/walle/proto/walleapi"
	"github.com/zviadm/walle/wallelib"
)

func cmdScan(
	ctx context.Context,
	rootPb *walleapi.Topology,
	clusterURI string,
	args []string) {
	f := flag.NewFlagSet("cmd.scan", flag.ExitOnError)
	entryId := f.Int("entry_id", -1, "EntryId to read. Can be -1 to read last-ish committed entry.")
	count := f.Int("c", 0, "Number of entries to read, if 0, will read till end.")
	// serverId := f.String("server_id", "", "Specific server_id to query. If empty, will choose random one.")
	f.Parse(args)
	args = f.Args()
	if len(args) != 1 {
		fmt.Println("must provide streamURI to read from.")
		os.Exit(1)
	}
	streamURI := args[0]

	cli, err := wallelib.NewClientFromRootPb(ctx, rootPb, clusterURI)
	exitOnErr(err)
	c, err := cli.ForStream(streamURI)
	exitOnErr(err)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.StreamEntries(streamCtx, &walleapi.StreamEntriesRequest{
		StreamUri:   streamURI,
		FromEntryId: int64(*entryId),
	})
	exitOnErr(err)
	if *count == 0 {
		*count = math.MaxInt64
	}
	for i := 0; i < *count; i++ {
		entry, err := stream.Recv()
		if err == io.EOF {
			return
		}
		exitOnErr(err)
		entryB, err := entry.Marshal()
		exitOnErr(err)
		if *count == 1 {
			fmt.Printf(
				"%d: w:%s checksum:%s\nDATA (%d): %v\nENCODED (%d): %v\n",
				entry.EntryId, hex.EncodeToString(entry.WriterId), hex.EncodeToString(entry.ChecksumMd5),
				len(entry.Data), entry.Data, len(entryB), entryB)
		} else {
			fmt.Printf(
				"%d: w:%s checksum:%s\n",
				entry.EntryId, hex.EncodeToString(entry.WriterId), hex.EncodeToString(entry.ChecksumMd5))
		}
	}
}
