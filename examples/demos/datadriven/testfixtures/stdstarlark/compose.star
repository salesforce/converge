# compose.star — the entrypoint the GENERIC stdstarlark composer calls:
# compose(spec, config). It fans a BOM out into a per-team children DAG, expressed as a
# MULTI-FILE Starlark program run by the shipped `stdstarlark` built-in.
#
# The point: stdstarlark assembles ALL .star files in the bundle (compose.star is the
# root entrypoint; every other .star is a loadable module via load()), so a program can
# split its logic across files — here the policies each live in their own policies/*.star
# `policy()` file, and the DAG emitters live in a shared emit.star module. The "gather
# policies + fan out" convention lives here in the .star program, not in Go — the
# provider stays generic. (Starlark load() needs a STRING-LITERAL module name, so the
# policy files are load()-ed explicitly by name; you can't iterate "all policy files"
# dynamically.)
#
#   - spec   = the resource's OWN spec — the BOM (deployment_instance shape).
#   - config = the effective providerconfig spec (unused here; the policies live in
#              .star files — but a program COULD read them from config instead, as the
#              generic compose(spec, config) contract allows).
#
# For every (policy, matching FD, team) it emits ONE of two dependency/value-flow DAGs,
# selected by the policy's `dag` field — the SAME DAGs celbom emits from its rules.
#
# Child (kind, name) is GLOBALLY unique (uniq_resource_meta), so names are prefixed
# "sstar-" — distinct from celbom ("cel-") and stdcel ("kro-").

load("emit.star", "emit_app", "emit_pipeline")
load("policies/stack.star", stack = "policy")
load("policies/pipeline.star", pipeline = "policy")

def compose(spec, config):
    # Gather the policies from their individual .star files (each defines policy()) —
    # the program owns this convention, not the provider.
    policies = [stack(), pipeline()]

    children = []
    edges = []
    di = spec.deployment_instance
    for fd in di.functional_domains:
        for p in policies:
            if not match(p.fd_selector, fd.name):
                continue
            for team in fd.service_teams:
                base = "sstar-%s-%s-%s" % (fd.name, team.name, p.name)
                labels = {"composed_by": "stdstarlark", "functional_domain": fd.name, "team": team.name, "policy": p.name}
                acct = "%s-%s" % (fd.name, team.name)  # the (fd, team) identity upstreams derive ids from
                if p.dag == "pipeline":
                    emit_pipeline(children, edges, base, team, labels)
                else:
                    emit_app(children, edges, base, acct, labels)
    return struct(children = children, edges = edges)
