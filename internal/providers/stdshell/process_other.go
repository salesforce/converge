//go:build !unix

package stdshell

import "os/exec"

// setProcessGroup is a no-op on non-Unix platforms: process-group signalling is a
// POSIX concept. exec.CommandContext's default Cancel (kill the direct child) plus
// WaitDelay still bound a runaway run; descendant reaping is best-effort here. The
// converge worker images are Linux, so this path exists only to keep the package
// cross-compilable.
func setProcessGroup(*exec.Cmd) {}
