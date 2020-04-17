module github.com/zviadm/walle

go 1.14

require (
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1
	github.com/zviadm/stats-go v0.0.3
	github.com/zviadm/stats-go/exporters/datadog v0.0.0-20200416122956-fa8cba8d81b7
	github.com/zviadm/stats-go/handlers/grpcstats v0.0.2-0.20200416122956-fa8cba8d81b7
	github.com/zviadm/tt v0.0.1
	github.com/zviadm/walle/proto v0.0.0
	github.com/zviadm/walle/wallelib v0.0.0
	github.com/zviadm/wt v0.0.0
	github.com/zviadm/zlog v0.0.0-20200326214804-bea93fc07ffa
	go.uber.org/atomic v1.6.0
	google.golang.org/genproto v0.0.0-20200413115906-b5235f65be36 // indirect
	google.golang.org/grpc v1.28.1
)

replace (
	github.com/zviadm/walle/proto => ./proto
	github.com/zviadm/walle/wallelib => ./wallelib
	github.com/zviadm/wt => ./wt
)
