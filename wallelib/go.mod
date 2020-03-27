module github.com/zviadm/walle/wallelib

go 1.12

require (
	github.com/zviadm/walle/proto v0.0.0
)

replace (
	github.com/zviadm/walle/proto => ./proto
)