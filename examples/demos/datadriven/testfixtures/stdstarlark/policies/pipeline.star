# A second policy, sitting ALONGSIDE stack.star — proof that a stdstarlark bundle
# spans multiple files. dag="pipeline" makes emit.star emit the
# fakek8sjob → faketerraform graph: faketerraform "runs" the team's *.tar bundle from S3
# and produces an image_tag; the engine flows it into the fakek8sjob's built_image (the
# Job runs the image the apply built). Adding this DAG for every team is a new .star file
# + a load() line in compose.star, no redeploy.
def policy():
    return struct(name = "pipeline", fd_selector = "fd-*", dag = "pipeline")
