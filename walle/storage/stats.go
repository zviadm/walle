package storage

import "github.com/zviadm/stats-go/metrics"

var committedIdGauge = metrics.DefineGauge(
	"walle/storage/committed_id", metrics.WithTags("stream_uri"))
var tailIdGauge = metrics.DefineGauge(
	"walle/storage/tail_id", metrics.WithTags("stream_uri"))
var gapStartIdGauge = metrics.DefineGauge(
	"walle/storage/gap_start_id", metrics.WithTags("stream_uri"))
