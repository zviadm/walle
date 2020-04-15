module github.com/zviadm/walle/wallelib

go 1.14

require (
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.3.0
	github.com/zviadm/walle/proto v0.0.0
	github.com/zviadm/zlog v0.0.0-20200326214804-bea93fc07ffa
	go.uber.org/atomic v1.6.0
	google.golang.org/grpc v1.28.0
)

replace github.com/zviadm/walle/proto => ../proto
