package pipeline

import "github.com/zviadm/stats-go/metrics"

var queueSizeGauge = metrics.DefineGauge(
	"walle/pipeline/queue_size", metrics.WithTags("stream_uri"))
var queueBytesGauge = metrics.DefineGauge(
	"walle/pipeline/queue_bytes", metrics.WithTags("stream_uri"))
var fforwardsCounter = metrics.DefineCounter(
	"walle/pipeline/fforwards", metrics.WithTags("stream_uri"))
