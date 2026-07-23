//go:build unix

package stdshell

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts the script in its OWN process group (Setpgid) and replaces
// exec.CommandContext's default Cancel — which signals only the direct child — with
// one that SIGKILLs the WHOLE group, so the script's descendants (the `sleep` in
// `bash -c 'sleep 30'`, a spawned tool) die with it rather than being orphaned.
// Signalling the negative PGID (syscall.Kill(-pgid, …)) is the POSIX idiom; the
// child is its own group leader (Setpgid), so its PID is the PGID.
//
// SIGKILL (not SIGTERM) is deliberate: Cancel fires only once the deadline/ctx has
// already elapsed, and the point is to free the worker slot promptly + close the
// output pipes so cmd.Wait returns. A descendant that inherited the pipe would
// otherwise block Wait until it exits on its own. (WaitDelay is the further backstop
// if even the group kill races.)
func setProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.Cancel = func() error {
		// Ignore ESRCH: the group may already be gone by the time Cancel runs.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
