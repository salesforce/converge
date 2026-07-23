package api

import (
	"context"
	"runtime"
)

// versionOutput is the GET /api/v1/version body: the running binary's stamped
// version plus the API versions this server speaks and the Go runtime it was
// built with. Lets a client (or an operator) read the deployed version over
// HTTP instead of inferring it from the cluster_members registry, and detect
// whether it talks the API version it expects.
type versionOutput struct {
	Body struct {
		// Version is the binary build string (e.g. a git describe / tag), or
		// "dev" for an unstamped local build.
		Version string `json:"version"`
		// APIVersion is the current/default API version this server serves
		// (the one under apiPrefix).
		APIVersion string `json:"api_version"`
		// APIVersions is every API version this server still serves. Today
		// just the current one; when a v2 lands alongside v1 both appear here so a
		// client can negotiate.
		APIVersions []string `json:"api_versions"`
		// GoVersion is the Go toolchain the binary was built with.
		GoVersion string `json:"go_version"`
	}
}

func (s *Server) getVersion(_ context.Context, _ *struct{}) (*versionOutput, error) {
	v := s.buildVersion
	if v == "" {
		v = "dev"
	}
	out := &versionOutput{}
	out.Body.Version = v
	out.Body.APIVersion = APIVersion
	out.Body.APIVersions = []string{APIVersion}
	out.Body.GoVersion = runtime.Version()
	return out, nil
}
