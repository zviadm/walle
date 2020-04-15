package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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
	fromEntryId := int64(*entryId)
	readN := 0
	for *count == 0 || readN < *count {
		stream, err := c.StreamEntries(streamCtx, &walleapi.StreamEntriesRequest{
			StreamUri:   streamURI,
			FromEntryId: fromEntryId,
		})
		exitOnErr(err)
		readNew := false
		for i := 0; ; i++ {
			entry, err := stream.Recv()
			if err == io.EOF {
				break
			}
			exitOnErr(err)
			readN += 1
			readNew = (i >= 1)
			fromEntryId = entry.EntryId
			if readN%10000 == 0 {
				fmt.Printf(
					"%d: w:%v checksum:%d\n",
					entry.EntryId, entry.WriterId, entry.ChecksumXX)
			}
		}
		if !readNew {
			break
		}
	}
	fmt.Printf("%d: read %d\n", fromEntryId, readN)
}
