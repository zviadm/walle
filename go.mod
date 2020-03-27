module github.com/zviadm/walle

go 1.12

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.3.5
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.5.1
	github.com/zviadm/walle/proto v0.0.0
	github.com/zviadm/walle/wallelib v0.0.0
	github.com/zviadm/wt v0.0.0
	github.com/zviadm/zlog v0.0.0-20200326214804-bea93fc07ffa
	golang.org/x/lint v0.0.0-20190313153728-d0100b6bd8b3 // indirect
	golang.org/x/tools v0.0.0-20190524140312-2c0ae7006135 // indirect
	google.golang.org/grpc v1.28.0
	honnef.co/go/tools v0.0.0-20190523083050-ea95bdfd59fc // indirect
)

replace (
	github.com/zviadm/walle/proto => ./proto
	github.com/zviadm/walle/wallelib => ./wallelib
	github.com/zviadm/wt => ./wt
)
