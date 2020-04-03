package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zviadm/walle/tt"
	"golang.org/x/mod/modfile"
)

var (
	verbose   = flag.Bool("v", false, "verbose output")
	maxMemory = flag.String("tt.memory", "1gb", "memory limit passed to docker run command")

	// go test flags
	runFlag   = flag.String("run", "", "see: go help testflag")
	benchFlag = flag.String("bench", "", "see: go help testflag")
	countFlag = flag.Int("count", 0, "see: go help testflag")
	raceFlag  = flag.Bool("race", false, "see: go help testflag")
	shortFlag = flag.Bool("short", false, "see: go help testflag")
)

type pkgConfig struct {
	MountDir     string
	GoModDir     string // Parent path that contains go.mod file
	RelativePath string // Relative path from GoModDir

	ModPath   string // Go module path
	GoVersion string // Go version defined for module
}

func pkgConfigFor(pkg string) (*pkgConfig, error) {
	pkgD, err := filepath.Abs(pkg)
	if err != nil {
		return nil, err
	}
	d := pkgD
	cfg := &pkgConfig{}
	for {
		if cfg.GoModDir == "" {
			if _, err := os.Stat(path.Join(d, "go.mod")); !os.IsNotExist(err) {
				cfg.GoModDir = d
				cfg.RelativePath = "." + pkgD[len(d):]
			}
		}
		if _, err := os.Stat(path.Join(d, ".git")); !os.IsNotExist(err) {
			cfg.MountDir = d
		}
		if d == "/" {
			break
		}
		d = path.Dir(d)
	}
	if cfg.GoModDir == "" {
		return nil, errors.New(fmt.Sprintf("go.mod not found for: %s", pkgD))
	}
	if cfg.MountDir == "" {
		cfg.MountDir = cfg.GoModDir
	}
	goModFile := path.Join(cfg.GoModDir, "go.mod")
	goModData, err := ioutil.ReadFile(goModFile)
	if err != nil {
		return nil, err
	}
	f, err := modfile.ParseLax(goModFile, goModData, nil)
	if err != nil {
		return nil, err
	}
	if f.Module.Mod.Path == "" {
		return nil, errors.New(fmt.Sprintf("module path not found in %s", goModFile))
	}
	if f.Go.Version == "" {
		return nil, errors.New(fmt.Sprintf("go version not found in %s", goModFile))
	}
	cfg.ModPath = f.Module.Mod.Path
	cfg.GoVersion = "go" + f.Go.Version
	return cfg, nil
}

func findPkgGroups(pkgs []string) (map[string][]*pkgConfig, error) {
	pkgGroups := make(map[string][]*pkgConfig)
	for _, pkg := range pkgs {
		cfg, err := pkgConfigFor(pkg)
		if err != nil {
			return nil, err
		}
		pkgGroups[cfg.ModPath] = append(pkgGroups[cfg.ModPath], cfg)
	}
	return pkgGroups, nil
}

func runTests(cacheDir string, pkgs []*pkgConfig) error {
	workDir := path.Join(tt.SourceDir, pkgs[0].GoModDir[len(pkgs[0].MountDir):])
	imgName := "tt-" + path.Base(pkgs[0].ModPath)
	args := []string{
		"run", "-i", "-t", "--rm",
		"--name", imgName,
		"-v", pkgs[0].MountDir + ":" + tt.SourceDir + ":cached",
		"-v", cacheDir + ":" + tt.CacheDir + ":delegated",
		"-w", workDir,
		"--memory", *maxMemory,
		"--memory-swap", *maxMemory,
		"--cap-add", "NET_ADMIN",
		imgName + ":latest",
		tt.GoBin(pkgs[0].GoVersion),
		"test", "-p", "1", "-failfast",
	}
	if *verbose {
		args = append(args, "-v")
	}
	if *raceFlag {
		args = append(args, "-race")
	}
	if *shortFlag {
		args = append(args, "-short")
	}
	if *runFlag != "" {
		args = append(args, "-run", *runFlag)
	}
	if *benchFlag != "" {
		args = append(args, "-bench", *benchFlag)
	}
	if *countFlag > 0 {
		args = append(args, "-count", strconv.Itoa(*countFlag))
	}
	for _, pkg := range pkgs {
		args = append(args, pkg.RelativePath)
	}

	cmd := exec.Command("docker", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if *verbose {
		fmt.Println("$:", "docker", strings.Join(args, " "))
	}
	return cmd.Run()
}

func main() {
	flag.Parse()
	pkgs := flag.Args()
	if len(pkgs) < 1 {
		fmt.Println("Must provide at least one package to test.")
		os.Exit(1)
	}
	pkgGroups, err := findPkgGroups(pkgs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cacheDir := path.Join(homeDir, ".tt_cache") // TODO(zviad): make this configurable?
	exitCode := 0
	for _, pkgs := range pkgGroups {
		err := runTests(cacheDir, pkgs)
		if err != nil {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
