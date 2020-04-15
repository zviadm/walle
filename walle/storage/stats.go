package storage

import "github.com/zviadm/stats-go/metrics"

var committedIdGauge = metrics.DefineGauge(
	"walle/storage/committed_id", metrics.WithTags("stream_uri"))
var gapStartIdGauge = metrics.DefineGauge(
	"walle/storage/gap_start_id", metrics.WithTags("stream_uri"))
var gapEndIdGauge = metrics.DefineGauge(
	"walle/storage/gap_end_id", metrics.WithTags("stream_uri"))
var tailIdGauge = metrics.DefineGauge(
	"walle/storage/tail_id", metrics.WithTags("stream_uri"))

var backfillsCounter = metrics.DefineCounter(
	"walle/storage/backfills", metrics.WithTags("stream_uri"))
var backfillBytesCounter = metrics.DefineCounter(
	"walle/storage/backfill_bytes", metrics.WithTags("stream_uri"))
