package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

func rootDirForPkg(pkg string) (string, string, error) {
	pkgD, err := filepath.Abs(pkg)
	if err != nil {
		return "", "", err
	}
	d := pkgD
	for {
		if d == "/" {
			return "", "", errors.New("Root path with `.git` folder not found!")
		}
		if _, err := os.Stat(path.Join(d, ".git")); !os.IsNotExist(err) {
			return d, "." + pkgD[len(d):], nil
		}
		d = path.Dir(d)
	}
}

func main() {
	flag.Parse()
	packages := flag.Args()
	if len(packages) < 1 {
		fmt.Println("Must provide at least one package to test.")
		os.Exit(1)
	}
	var rootDir string
	for idx, pkg := range packages {
		root, pkgD, err := rootDirForPkg(pkg)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if rootDir == "" {
			rootDir = root
		} else if rootDir != root {
			fmt.Printf("Packages from different roots can't be tested together: %s vs %s\n", rootDir, root)
			os.Exit(1)
		}
		packages[idx] = pkgD
	}
	cacheDir, err := os.UserHomeDir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cacheDir = path.Join(cacheDir, ".tt_cache")
	args := []string{
		"run", "-i", "-t",
		"-v", rootDir + ":/root/src:cached",
		"-v", cacheDir + ":/root/.cache:delegated",
		"-w", "/root/src",
		"walle:latest", // TODO(zviad): control with a cfg/flag
		"/root/go1.4/bin/go",
		"test", "-v",
	}
	for _, pkg := range packages {
		args = append(args, pkg)
	}
	cmd := exec.Command("docker", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Println("$:", "docker", strings.Join(args, " "))
	if err := cmd.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
