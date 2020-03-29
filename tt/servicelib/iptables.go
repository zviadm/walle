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

func IptablesClearAll(t *testing.T) {
	out, err := exec.Command("iptables", "-F").CombinedOutput()
	require.NoError(t, err, string(out))
}

func IptablesBlockPort(t *testing.T, port int) {
	zlog.Info("TEST: iptables: blocking port ", port)
	out, err := exec.Command(
		"iptables", "-I", "INPUT",
		"-p", "tcp", "--dport", strconv.Itoa(port),
		"-i", "lo", "-j", "DROP").CombinedOutput()
	require.NoError(t, err, string(out))
}

func IptablesUnblockPort(t *testing.T, port int) {
	zlog.Info("TEST: iptables: unblocking port ", port)
	out, err := exec.Command("iptables", "-D", "INPUT", "1").CombinedOutput()
	require.NoError(t, err, string(out))
}
