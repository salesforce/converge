#!/bin/bash
# LocalStack init hook — runs automatically INSIDE the LocalStack container once S3
# is ready (mounted at /etc/localstack/init/ready.d/). It seeds the demo's S3: the
# Terraform state backend bucket, the HCL bundle bucket, and uploads the bundle a
# terraform resource points at. `awslocal` (LocalStack's pre-authed aws wrapper) is
# built into the image, so this needs no host aws CLI and no seeding race — the
# hook fires exactly when S3 can serve requests.
#
# The bundle is mounted read-only at /seed/bundle.tar.gz (see docker-compose.yml);
# `just demo` regenerates it from hcl-bundle/*.tf before `compose up`.
set -euo pipefail

echo "terraform init: seeding S3"
awslocal s3 mb s3://tf-state    || true   # terraform state backend
awslocal s3 mb s3://tf-bundles  || true   # HCL bundle store
awslocal s3 cp /seed/bundle.tar.gz s3://tf-bundles/hello.tar.gz
echo "terraform init: seeded s3://tf-bundles/hello.tar.gz + empty s3://tf-state"
