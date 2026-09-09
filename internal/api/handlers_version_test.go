package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetVersion(t *testing.T) {
	t.Run("reports the stamped build version + API version", func(t *testing.T) {
		srv := &Server{}
		srv.SetBuildVersion("v1.2.3-abc")
		out, err := srv.getVersion(context.Background(), &struct{}{})
		require.NoError(t, err)
		require.Equal(t, "v1.2.3-abc", out.Body.Version)
		require.Equal(t, APIVersion, out.Body.APIVersion)
		require.Equal(t, []string{APIVersion}, out.Body.APIVersions)
		require.Equal(t, runtime.Version(), out.Body.GoVersion)
	})

	t.Run("unstamped build defaults to dev", func(t *testing.T) {
		out, err := (&Server{}).getVersion(context.Background(), &struct{}{})
		require.NoError(t, err)
		require.Equal(t, "dev", out.Body.Version)
	})
}

func TestAPIVersionHeaderMiddleware(t *testing.T) {
	h := apiVersionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/resources", nil))
	require.Equal(t, APIVersion, rec.Header().Get("X-API-Version"),
		"every response must advertise the API version")
}
