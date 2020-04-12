package panic

import "fmt"

// OnErr panics if err != nil.
func OnErr(err error) {
	if err != nil {
		panic(err)
	}
}

// OnNotOk panics if ok != True.
func OnNotOk(ok bool, msg string, args ...interface{}) {
	if !ok {
		panic(fmt.Sprintf(msg, args...))
	}
}
