module github.com/zviadm/walle

go 1.14

require (
	github.com/golang/protobuf v1.3.5
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1
	github.com/zviadm/stats-go v0.0.2
	github.com/zviadm/stats-go/exporters/datadog v0.0.0-20200412142242-c3dc8e03d31d
	github.com/zviadm/stats-go/handlers/grpcstats v0.0.0-20200412142242-c3dc8e03d31d
	github.com/zviadm/tt v0.0.0
	github.com/zviadm/walle/proto v0.0.0
	github.com/zviadm/walle/wallelib v0.0.0
	github.com/zviadm/wt v0.0.0
	github.com/zviadm/zlog v0.0.0-20200326214804-bea93fc07ffa
	go.uber.org/atomic v1.6.0
	google.golang.org/grpc v1.28.1
)

replace (
	github.com/zviadm/tt => ./tt
	github.com/zviadm/walle/proto => ./proto
	github.com/zviadm/walle/wallelib => ./wallelib
	github.com/zviadm/wt => ./wt
)
