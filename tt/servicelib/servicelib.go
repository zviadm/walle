package servicelib

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
)

var goVer = runtime.Version()

type Service struct {
	cmd          *exec.Cmd
	cv           *sync.Cond
	processState *os.ProcessState
}

func RunGoService(
	ctx context.Context,
	pkg string,
	flags []string,
	waitOnPort string) (*Service, error) {

	cmd := exec.Command(path.Join("/root", goVer, "bin/go"), "install", pkg)
	glog.Infof("running: %s", cmd)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	cmd = exec.Command(path.Join("/root/.cache/goroot/bin", path.Base(pkg)), flags...)
	glog.Infof("running: %s", cmd)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
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
func (s *Service) Wait(t *testing.T) {
	s.cv.L.Lock()
	defer s.cv.L.Unlock()
	for s.processState == nil {
		s.cv.Wait()
	}
	require.EqualValues(t, 0, s.processState.ExitCode())
}
func (s *Service) Stop(t *testing.T) {
	if s.IsDone() == nil {
		err := s.cmd.Process.Signal(syscall.SIGTERM)
		require.NoError(t, err)
	}
	s.Wait(t)
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
