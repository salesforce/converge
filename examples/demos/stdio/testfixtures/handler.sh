#!/usr/bin/env bash
# handler.sh — the CANONICAL reference implementation of the converge stdio
# protocol, in bash. bin/stdworker runs this once per task with the task JSON on
# stdin and reads one Response JSON from stdout. Port this contract to any language
# to plug a non-Go worker into converge — it is the whole interface.
#
# The converge stdio protocol (see internal/providers/stdio/types.go for the Go structs):
#   stdin  ← Request  {"reaction","trigger","kind","name","generation","spec","config","bundle"}
#   stdout → Response {"status":{...},"conditions":[...],"children":[...],"edges":[...]}
#   exit 0 = success | exit 2 = terminal (no retry) | other non-zero = transient (retried)
#   Anything on stderr is captured by the worker for logs / the failure message.
#
# This handler serves BOTH kinds the stdio provider exposes, branching on .reaction:
#   - "work"    (kind stdio):          runs spec.run as a shell command, records the
#                                      exit code + stdout tail in status. "Run arbitrary bash."
#   - "compose" (kind stdio-composer): fans out one child per name in spec.children, all of
#                                      the child kind spec.child_kind (default "stdio") at
#                                      spec.child_kind_version (default 1), each child named
#                                      "<spec.name_prefix>-<entry>".
#
# It uses `jq` for JSON (ubiquitous, no compiled deps). Keep stdout PURE JSON —
# send every diagnostic to stderr (>&2) so it never corrupts the Response.
set -euo pipefail

req="$(cat)" # the whole Request JSON

reaction="$(jq -r '.reaction // "work"' <<<"$req")"
name="$(jq -r '.name // "?"' <<<"$req")"
echo "handler: reaction=$reaction name=$name" >&2

case "$reaction" in
work)
	# Leaf work: run spec.run (a shell command string) and capture its result.
	cmd="$(jq -r '.spec.run // empty' <<<"$req")"
	if [[ -z "$cmd" ]]; then
		echo "handler: spec.run is required for the 'stdio' work kind" >&2
		exit 2 # terminal: a same-generation retry can't add a missing field
	fi
	# Run it; capture stdout + exit code. A non-zero command exit is reflected in
	# status (the task still SUCCEEDS — the reconcile ran; the command's own result
	# is data). Exit non-zero from THIS handler only for infra failures you want retried.
	out="$(bash -c "$cmd" 2>&1)" && rc=0 || rc=$?
	# Emit status: the command, its exit code, and a bounded stdout tail (last 4 KB
	# via tail -c — portable, unlike bash 3.2's negative-offset substring).
	tail_out="$(printf '%s' "$out" | tail -c 4000)"
	jq -n --arg run "$cmd" --argjson rc "$rc" --arg out "$tail_out" \
		'{status: {run: $run, exit_code: $rc, output: $out, ok: ($rc == 0)}}'
	;;

compose)
	# Composer: one child per spec.children[], all of kind spec.child_kind at
	# spec.child_kind_version. Each child's spec carries a `run` so a `stdio` leaf
	# child does real work.
	#
	# child_kind_version is REQUIRED on every emitted child (no implicit default) —
	# the composer pins the exact (kind, version) each child is applied at.
	#
	# A child's (kind, name) is GLOBALLY unique, so names are prefixed with
	# spec.name_prefix. Like every composer, the prefix comes from the resource's OWN
	# spec — the reconcile path does not carry the resource name (it lives in a side
	# table), so a composer names its children from data it holds: the spec.
	child_kind="$(jq -r '.spec.child_kind // "stdio"' <<<"$req")"
	child_kv="$(jq -r '.spec.child_kind_version // 1' <<<"$req")"
	prefix="$(jq -r '.spec.name_prefix // "child"' <<<"$req")"
	jq -c \
		--arg ck "$child_kind" \
		--argjson kv "$child_kv" \
		--arg prefix "$prefix" \
		'{
      children: [
        (.spec.children // [])[] | {
          kind:         $ck,
          kind_version: $kv,
          name:         ($prefix + "-" + .),
          spec:         {run: ("echo composed child " + .)}
        }
      ],
      status: {composed: ((.spec.children // []) | length)}
    }' <<<"$req"
	;;

*)
	echo "handler: unknown reaction $reaction" >&2
	exit 2
	;;
esac
