package walle

import "github.com/zviadm/stats-go/metrics"

var heartbeatsCounter = metrics.DefineCounter(
	"walle/server/heartbeats", metrics.WithTags("stream_uri"))
var requestsRejectedC = metrics.DefineCounter(
	"walle/server/requests_rejected").V(nil)
