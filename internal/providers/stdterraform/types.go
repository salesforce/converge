package stdterraform

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the FIXED kind this provider serves — "stdterraform". Its reference CRD
// lives beside this package (stdterraform.kind.json). The provider serves exactly this one kind;
// there is no env var to rename it.
const Kind converge.Kind = "stdterraform"

// Finalizer is the finalizer string the teardown reaction strips on a successful
// `terraform destroy`. The core holds the resource in Deleting until the destroy
// completes, so the real cloud infrastructure is torn down before the row is removed.
const Finalizer = "converge.dev/stdterraform"

// Backend names a supported Terraform state backend. It is the discriminator on
// Config that selects which backend block the worker generates (with that cloud's
// NATIVE state locking). The raw-HCL escape hatch (a backend.tf in the providerconfig
// bundle) bypasses this entirely — see Config.
type Backend string

const (
	// BackendS3 is AWS S3 (or an S3-compatible store like LocalStack/MinIO). Locking
	// is NATIVE: the worker emits `use_lockfile = true` (a `.tflock` object beside the
	// state — no DynamoDB table). Requires Terraform/OpenTofu ≥ 1.10.
	BackendS3 Backend = "s3"
	// BackendAzureRM is Azure Blob Storage (azurerm). Locking is AUTOMATIC via native
	// blob leases — nothing to configure.
	BackendAzureRM Backend = "azurerm"
	// BackendGCS is Google Cloud Storage (gcs). Locking is AUTOMATIC via an atomic
	// lock object — nothing to configure.
	BackendGCS Backend = "gcs"
)

// Spec is a terraform resource's desired state: WHERE the Terraform module lives and
// the input variables to apply it with. The module is fetched per task (S3 tarball,
// HTTPS tarball, or a git repo — see Source), unpacked, and applied with `-var`
// flags built from Vars. The spec carries NO backend/state config — that is the
// kind's providerconfig, identical for every resource of the kind (the resource is
// the "what to build", like a Terraform Enterprise workspace's module + variables).
type Spec struct {
	// Source is the module location. One of:
	//   - s3://bucket/key.tar.gz                 — a *.tar.gz from AWS S3 (aws CLI).
	//   - gs://bucket/key.tar.gz                 — a *.tar.gz from GCS (gcloud storage).
	//   - azblob://account/container/key.tar.gz  — a *.tar.gz from Azure Blob (az CLI).
	//   - https://host/path/mod.tar.gz           — a *.tar.gz fetched over HTTPS (curl).
	//   - git::https://host/repo[//subdir][?ref=REF] — a git repo cloned at REF; the
	//     optional //subdir selects a module directory within the repo.
	// The scheme selects the fetch method (each object-store scheme uses that cloud's
	// ambient-identity CLI — no creds in the spec); anything else fails TERMINALLY.
	Source string `json:"source" doc:"Module location: s3://…tar.gz, gs://…tar.gz, azblob://acct/container/…tar.gz, https://…tar.gz, or git::https://repo[//subdir][?ref=REF]."`
	// Vars are Terraform input variables passed to the module as -var flags. Free-form
	// so a module can declare any variables it wants.
	Vars map[string]string `json:"vars,omitempty" doc:"Terraform input variables (-var name=value) for the module."`
	// TFVersion optionally pins the Terraform/OpenTofu version this resource runs.
	// The worker resolves the version via tfenv (binary=terraform) / tofuenv
	// (binary=tofu), so it AUTO-INSTALLS the right toolchain per resource. Accepts any
	// tfenv/tofuenv token:
	//   - an exact version, e.g. "1.7.5"
	//   - "latest" / "latest:<regex>" (e.g. "latest:^1.7")
	//   - "min-required"  — the floor the module's required_version declares
	//   - "latest-allowed"— the newest version the module's required_version permits
	// EMPTY (the default) = AUTO: honor a `.terraform-version`/`.opentofu-version` file
	// shipped in the bundle if present, else "latest-allowed" (newest the module's
	// required_version allows; falls back to latest when the module declares none).
	TFVersion string `json:"tf_version,omitempty" doc:"Pin the Terraform/OpenTofu version (tfenv/tofuenv token: exact e.g. 1.7.5, latest, latest:<regex>, min-required, latest-allowed). Empty = auto (bundle .terraform-version/.opentofu-version, else latest-allowed)."`
}

// Status is the applied result: the Terraform outputs (name → string) the module
// declared, captured from `terraform output -json`. A downstream consumer can
// value-flow a field out of here (e.g. a created bucket ARN) into a dependent's spec.
type Status struct {
	// Outputs is the module's Terraform outputs, flattened to name → string
	// (non-string values are kept as JSON so any output shape round-trips).
	Outputs map[string]string `json:"outputs,omitempty" doc:"Terraform outputs the module produced (name → value)."`
	// Applied is set true once `terraform apply` succeeds.
	Applied bool `json:"applied" doc:"True once terraform apply has succeeded."`
}

// Config is the terraform kind's providerconfig — the WHERE-to-run-and-store-state
// settings shared by every resource of the kind (the operator configures the state
// backend ONCE, like a Terraform Enterprise workspace's state settings). Read live
// per task, so a backend change lands with no worker restart. Credentials are NEVER
// here — each backend reads the cloud's standard credential chain (AWS_*, ARM_*,
// GOOGLE_*) the worker process inherits (env / instance profile / workload identity).
//
// Two ways to specify the backend, in precedence order:
//  1. RAW HCL (escape hatch): put a full `backend.tf` in the providerconfig `data`
//     bundle to use ANY backend Terraform supports (oss, http, pg, consul, custom
//     endpoints, KMS keys, …). It MUST use the placeholder StateKeyPlaceholder where
//     the per-resource state key goes (the bundle is kind-wide; the worker substitutes
//     the per-resource key so resources don't clobber one state file). Wins if present.
//  2. TYPED discriminator: set Backend (s3|azurerm|gcs) + that backend's fields below;
//     the worker generates the block with the cloud's native locking auto-on.
//
// The state key is ALWAYS per-resource (derived from StatePrefix + kind + the
// resource UUID), so many resources share one backend location without colliding —
// each is its own state + its own lock (a workspace of one).
type Config struct {
	// Backend selects the generated state backend: "s3" | "azurerm" | "gcs". Ignored
	// when a raw backend.tf is supplied via the bundle. Default "s3".
	Backend Backend `json:"backend,omitempty" doc:"State backend to generate: s3 | azurerm | gcs (ignored if a raw backend.tf is in the providerconfig bundle). Default s3."`

	// StatePrefix is prepended to each resource's state key/path. Default "terraform/".
	// The final key is "<state_prefix><kind>/<resource-uuid>.tfstate".
	StatePrefix string `json:"state_prefix,omitempty" doc:"Key/path prefix for state objects (default terraform/); the per-resource UUID is appended."`

	// Binary selects the CLI FAMILY and thus the version manager the worker resolves
	// through — NOT a fixed binary:
	//   - "terraform" (default) → HashiCorp Terraform, version-managed by tfenv
	//     (reads the module's required_version + `.terraform-version`). BUSL-1.1.
	//   - "tofu"                → OpenTofu, version-managed by tofuenv
	//     (reads required_version + `.opentofu-version`). MPL-2.0.
	// The worker AUTO-INSTALLS the resolved version per resource (see Spec.TFVersion),
	// so the image ships tfenv + tofuenv, not a pinned CLI. Note: the s3 backend's
	// native lock (use_lockfile) needs terraform/tofu ≥ 1.10, so pin/allow ≥ 1.10 when
	// using backend=s3.
	Binary string `json:"binary,omitempty" doc:"CLI family: terraform (HashiCorp, via tfenv) or tofu (OpenTofu, via tofuenv). Default terraform. Selects the version manager; the version itself is auto-resolved (see spec.tf_version)."`

	// S3 holds the AWS S3 backend fields (used when Backend is "s3" or empty).
	S3 S3Backend `json:"s3" doc:"AWS S3 backend settings (when backend=s3)."`
	// AzureRM holds the Azure Blob backend fields (used when Backend is "azurerm").
	AzureRM AzureRMBackend `json:"azurerm" doc:"Azure Blob (azurerm) backend settings (when backend=azurerm)."`
	// GCS holds the Google Cloud Storage backend fields (used when Backend is "gcs").
	GCS GCSBackend `json:"gcs" doc:"Google Cloud Storage (gcs) backend settings (when backend=gcs)."`
}

// S3Backend configures the AWS S3 state backend. Locking is native (`use_lockfile`);
// no DynamoDB table. Credentials come from the AWS default chain (AWS_* env / instance
// profile / IRSA), never from here.
type S3Backend struct {
	// Bucket is the S3 bucket the state lives in. REQUIRED for backend=s3.
	Bucket string `json:"bucket,omitempty" doc:"S3 bucket for the Terraform state (required for backend=s3)."`
	// Region is the bucket's AWS region. Default us-east-1 (also honours AWS_REGION).
	Region string `json:"region,omitempty" doc:"AWS region of the state bucket (default us-east-1)."`
	// Endpoint overrides the S3 endpoint for an S3-compatible store — a LocalStack/
	// MinIO URL (e.g. http://localstack:4566). Empty = real AWS. When set, the worker
	// also emits the skip_*/use_path_style flags the non-AWS S3 API needs.
	Endpoint string `json:"endpoint,omitempty" doc:"S3 endpoint override for an S3-compatible store (e.g. http://localstack:4566). Empty = real AWS."`
}

// AzureRMBackend configures the Azure Blob (azurerm) state backend. Locking is
// automatic (blob lease). Auth comes from the ARM_* env / workload identity chain
// (e.g. ARM_USE_AKS_WORKLOAD_IDENTITY + use_azuread_auth), never from here.
type AzureRMBackend struct {
	// StorageAccount is the Azure Storage account name. REQUIRED for backend=azurerm.
	StorageAccount string `json:"storage_account,omitempty" doc:"Azure Storage account name (required for backend=azurerm)."`
	// Container is the blob container the state lives in. REQUIRED for backend=azurerm.
	Container string `json:"container,omitempty" doc:"Blob container for the state (required for backend=azurerm)."`
	// ResourceGroup is the storage account's resource group. Required unless using
	// Entra ID (AzureAD) data-plane auth.
	ResourceGroup string `json:"resource_group,omitempty" doc:"Resource group of the storage account (required unless using AzureAD auth)."`
	// UseAzureADAuth uses Entra ID (workload identity) to reach the state blob instead
	// of a storage account key — the recommended path for an identity-bearing worker.
	UseAzureADAuth bool `json:"use_azuread_auth,omitempty" doc:"Use Entra ID (workload identity) for the state blob instead of an account key."`
}

// GCSBackend configures the Google Cloud Storage (gcs) state backend. Locking is
// automatic (atomic lock object). Auth comes from Application Default Credentials
// (GOOGLE_* env / GKE Workload Identity), never from here.
type GCSBackend struct {
	// Bucket is the GCS bucket the state lives in. REQUIRED for backend=gcs.
	Bucket string `json:"bucket,omitempty" doc:"GCS bucket for the Terraform state (required for backend=gcs)."`
}
