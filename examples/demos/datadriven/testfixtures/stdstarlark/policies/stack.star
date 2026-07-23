# A policy: returns one struct {name, fd_selector, dag}. compose.star load()s this file
# and calls policy() to collect it. fd_selector is a glob matched against each
# functional domain's name; `dag`
# selects WHICH dependency graph emit.star emits ("app" or "pipeline"). This one is the
# default landing-zone footprint: dag="app" → fakeapp depends on fakevpc + fakedb.
def policy():
    return struct(name = "stack", fd_selector = "fd-*", dag = "app")
