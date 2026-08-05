# emit.star — the two DAG emitters, load()-ed by compose.star. Splitting them into a
# module keeps the DAG SHAPE separate from the policy SELECTORS (policies/*.star) and
# the fan-out loop (compose.star).

# emit_app: the fakeapp → (fakevpc, fakedb) DAG. The two upstreams derive their
# produced id/endpoint from their account_id; fakeapp's vpc_id + db_endpoint are LEFT
# ABSENT (the upstreams fill them via the two value flows before fakeapp is scheduled).
def emit_app(children, edges, base, acct, labels):
    app = base + "-app"
    vpc = base + "-vpc"
    db = base + "-db"
    # kind_version is REQUIRED and explicit (>= 1) on every emitted child — no implicit v1 default.
    children.append(struct(kind = "fakeapp", kind_version = 1, name = app, spec = {"functional_domain": labels["functional_domain"], "team": labels["team"]}, labels = labels))
    children.append(struct(kind = "fakevpc", kind_version = 1, name = vpc, spec = {"account_id": acct}, labels = labels))
    children.append(struct(kind = "fakedb", kind_version = 1, name = db, spec = {"account_id": acct}, labels = labels))
    edges.append(struct(
        dependent = struct(kind = "fakeapp", name = app),
        dependency = struct(kind = "fakevpc", name = vpc),
        values = [struct(dependent = "/vpc_id", source = "/vpc_id")],
    ))
    edges.append(struct(
        dependent = struct(kind = "fakeapp", name = app),
        dependency = struct(kind = "fakedb", name = db),
        values = [struct(dependent = "/db_endpoint", source = "/db_endpoint")],
    ))

# emit_pipeline: the fakek8sjob → faketerraform DAG. faketerraform "runs" the team's
# *.tar bundle from S3 (tar_url baked in here) and produces an image_tag; fakek8sjob
# runs a literal image but ALSO depends on the apply — the engine flows the built
# image_tag into the job's built_image (built wins). built_image is LEFT ABSENT.
def emit_pipeline(children, edges, base, team, labels):
    job = base + "-job"
    tf = base + "-tf"
    children.append(struct(kind = "fakek8sjob", kind_version = 1, name = job, spec = {"image": "nginx:1.27"}, labels = labels))
    children.append(struct(kind = "faketerraform", kind_version = 1, name = tf, spec = {"tar_url": "s3://team-bundles/%s.tar" % team.name}, labels = labels))
    edges.append(struct(
        dependent = struct(kind = "fakek8sjob", name = job),
        dependency = struct(kind = "faketerraform", name = tf),
        values = [struct(dependent = "/built_image", source = "/image_tag")],
    ))
