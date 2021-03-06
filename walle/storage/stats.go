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
var backfillTotalMsCounter = metrics.DefineCounter(
	"walle/storage/backfill_total_ms", metrics.WithTags("stream_uri"))

var trimsCounter = metrics.DefineCounter(
	"walle/storage/trims", metrics.WithTags("stream_uri"))
var trimTotalMsCounter = metrics.DefineCounter(
	"walle/storage/trim_total_ms", metrics.WithTags("stream_uri"))

var streamCursorsGauge = metrics.DefineGauge(
	"walle/storage/cursors", metrics.WithTags("stream_uri"))
var cursorNextsCounter = metrics.DefineCounter(
	"walle/storage/cursor_nexts", metrics.WithTags("stream_uri"))
