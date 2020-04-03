package tt

import "path"

const (
	SourceDir = "/root/src"    // path where source code is mounted inside container
	CacheDir  = "/root/.cache" // path where cache directory is mounted inside container
)

func GoBin(ver string) string {
	return path.Join("/root", ver, "bin/go")
}
