package panic

import "fmt"

func OnErr(err error) {
	if err != nil {
		panic(err)
	}
}
func OnNotOk(ok bool, msg string, args ...interface{}) {
	if !ok {
		panic(fmt.Sprintf(msg, args...))
	}
}
