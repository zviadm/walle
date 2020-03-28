package servicelib

import (
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zviadm/zlog"
)

// List rules: iptables --list -n
// Add rule: iptables -I INPUT -p tcp --dport 5005 -i lo -j DROP
// Del rule: iptables -D INPUT 1

func IptablesBlockPort(t *testing.T, port int) {
	zlog.Info("iptables: blocking port ", port)
	err := exec.Command(
		"iptables", "-I", "INPUT",
		"-p", "tcp", "--dport", strconv.Itoa(port),
		"-i", "lo", "-j", "DROP").Run()
	require.NoError(t, err)
}

func IptablesUnblockPort(t *testing.T, port int) {
	zlog.Info("iptables: unblocking port ", port)
	err := exec.Command(
		"iptables", "-D", "INPUT", "1").Run()
	require.NoError(t, err)
}
