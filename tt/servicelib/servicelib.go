package servicelib

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/zlog"
)

var goVer = runtime.Version()

var installMx sync.Mutex
var pkgsInstalled = make(map[string]struct{})

type Service struct {
	cmd          *exec.Cmd
	cv           *sync.Cond
	processState *os.ProcessState
}

type serviceLogger struct {
	Prefix string
}

func (s *serviceLogger) Write(b []byte) (int, error) {
	prevIdx := 0
	for idx := 0; idx < len(b); idx++ {
		if b[idx] != '\n' {
			continue
		}
		os.Stderr.WriteString(s.Prefix + string(b[prevIdx:idx+1]))
		prevIdx = idx + 1
	}
	if prevIdx != len(b) {
		os.Stderr.WriteString(s.Prefix + string(b[prevIdx:]))
	}
	return len(b), nil
}

func installPkg(pkg string) error {
	installMx.Lock()
	defer installMx.Unlock()
	if _, ok := pkgsInstalled[pkg]; ok {
		return nil
	}
	cmd := exec.Command(path.Join("/root", goVer, "bin/go"), "install", pkg)
	zlog.Infof("running: %s", cmd)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	pkgsInstalled[pkg] = struct{}{}
	return nil
}

func RunGoService(
	ctx context.Context,
	pkg string,
	flags []string,
	waitOnPort string) (*Service, error) {

	if err := installPkg(pkg); err != nil {
		return nil, err
	}
	cmd := exec.Command(path.Join("/root/.cache/goroot/bin", path.Base(pkg)), flags...)
	zlog.Infof("running: %s", cmd)
	cmd.Stdin = os.Stdin
	cmd.Stderr = &serviceLogger{Prefix: fmt.Sprintf("%s%5s   ", path.Base(pkg), waitOnPort)}
	cmd.Stdout = cmd.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &Service{cmd: cmd, cv: sync.NewCond(new(sync.Mutex))}
	go func() {
		<-ctx.Done()
		cmd.Process.Kill()
	}()
	go func() {
		err := cmd.Wait()
		if err != nil && cmd.ProcessState == nil {
			panic(err) // This must never happen.
		}
		s.cv.L.Lock()
		defer s.cv.L.Unlock()
		s.processState = cmd.ProcessState
		s.cv.Broadcast()
	}()

	if waitOnPort != "" {
		if err := s.waitForPort(ctx, waitOnPort); err != nil {
			_ = s.cmd.Process.Kill()
			return nil, err
		}
	}
	return s, nil
}

func (s *Service) IsDone() *os.ProcessState {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()
	return s.processState
}
func (s *Service) wait() int {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()
	for s.processState == nil {
		s.cv.Wait()
	}
	return s.processState.ExitCode()
}
func (s *Service) Wait(t *testing.T) {
	eCode := s.wait()
	require.EqualValues(t, 0, eCode)
}
func (s *Service) Stop(t *testing.T) {
	if s.IsDone() == nil {
		err := s.cmd.Process.Signal(syscall.SIGTERM)
		require.NoError(t, err)
	}
	s.Wait(t)
}
func (s *Service) Kill(t *testing.T) {
	if s.IsDone() != nil {
		return
	}
	err := s.cmd.Process.Kill()
	require.NoError(t, err)
	_ = s.wait()
}

func (s *Service) waitForPort(ctx context.Context, port string) error {
	d := net.Dialer{}
	for {
		conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort("", port))
		if err != nil && ctx.Err() == nil && s.IsDone() == nil {
			continue
		}
		if err != nil {
			return err
		}
		return conn.Close()
	}
}
