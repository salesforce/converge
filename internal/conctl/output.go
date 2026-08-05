package conctl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// Output format flag values.
const (
	outputTable = "table"
	outputJSON  = "json"
	outputYAML  = "yaml"
)

// column is one table column: a header and an extractor from a row value.
type column[T any] struct {
	header string
	value  func(T) string
}

// render writes rows in the requested format. For table it uses the supplied
// columns; for json/yaml it marshals the raw payload (so the machine-readable
// output preserves the full API shape, not the trimmed table view). payload is
// what json/yaml serialize — pass the API body (or a slice of it), NOT the
// table rows, so `-o json` round-trips the real object.
func render[T any](o *globalOpts, payload any, rows []T, cols []column[T]) error {
	switch o.output {
	case outputJSON:
		return writeJSON(os.Stdout, payload)
	case outputYAML:
		return writeYAML(os.Stdout, payload)
	default:
		return writeTable(os.Stdout, rows, cols)
	}
}

// renderOne renders a single object (get by kind/name). Table form prints a
// key/value block rather than a one-row wide table, which reads better for a
// single object; json/yaml marshal the payload verbatim.
func renderOne(o *globalOpts, payload any, fields []kv) error {
	switch o.output {
	case outputJSON:
		return writeJSON(os.Stdout, payload)
	case outputYAML:
		return writeYAML(os.Stdout, payload)
	default:
		return writeKV(os.Stdout, fields)
	}
}

// kv is one label/value line in a single-object table view.
type kv struct {
	k string
	v string
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}

func writeYAML(w io.Writer, v any) error {
	// sigs.k8s.io/yaml marshals via the json tags, so the YAML keys match the
	// API's json field names (snake_case) rather than Go field names.
	b, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode yaml: %w", err)
	}
	_, err = w.Write(b)
	return err
}

func writeTable[T any](w io.Writer, rows []T, cols []column[T]) error {
	tw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
	headers := make([]string, len(cols))
	for i, c := range cols {
		headers[i] = c.header
	}
	// Writes to the buffered tabwriter can't fail usefully; Flush surfaces the
	// only actionable error (the underlying io.Writer). Discard the intermediate
	// write errors explicitly.
	_, _ = fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, r := range rows {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = c.value(r)
		}
		_, _ = fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(rows) == 0 {
		_, _ = fmt.Fprintln(w, "No resources found.")
	}
	return nil
}

func writeKV(w io.Writer, fields []kv) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, f := range fields {
		_, _ = fmt.Fprintf(tw, "%s:\t%s\n", f.k, f.v)
	}
	return tw.Flush()
}

// okBody resolves a generated response into its typed body, tolerant of the
// API's 200-vs-201 distinction. oapi-codegen only populates the typed JSON200
// field when the status is exactly 200; the converge apply endpoints return
// 201 Created on first create (with X-Apply-Result: created), which the spec
// does not enumerate, so the body lands in raw .Body instead. This helper:
//   - returns typed if the client already decoded it (200), else
//   - if the status is any 2xx, unmarshals raw into a fresh T, else
//   - returns the decoded RFC7807 problem as an error.
//
// It keeps the CLI resilient to every 2xx the server actually sends without a
// per-status-code spec change. typed is the client's JSON200 pointer (may be
// nil); raw is resp.Body; httpResp is resp.HTTPResponse; problem is the typed
// default error body.
func okBody[T any](typed *T, raw []byte, httpResp *http.Response, problem *apiclient.ErrorModel) (*T, error) {
	if typed != nil {
		return typed, nil
	}
	if httpResp != nil && httpResp.StatusCode >= 200 && httpResp.StatusCode < 300 {
		var out T
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &out); err != nil {
				return nil, fmt.Errorf("decode %s response: %w", httpResp.Status, err)
			}
		}
		return &out, nil
	}
	return nil, apiError(httpResp, problem)
}

// apiError turns a non-2xx typed response into a Go error carrying the RFC7807
// problem detail the API returns (title/detail/status), so a failed apply reads
// like `Error: 422 Unprocessable Entity: spec: does not match config_schema`.
// httpResp is the raw response (for the status line); problem is the decoded
// ErrorModel (may be nil if the body wasn't the problem shape).
func apiError(httpResp *http.Response, problem *apiclient.ErrorModel) error {
	status := "request failed"
	if httpResp != nil {
		status = httpResp.Status
	}
	if problem == nil {
		return fmt.Errorf("%s", status)
	}
	var msg strings.Builder
	if problem.Title != nil {
		msg.WriteString(*problem.Title)
	}
	if problem.Detail != nil && *problem.Detail != "" {
		if msg.Len() > 0 {
			msg.WriteString(": ")
		}
		msg.WriteString(*problem.Detail)
	}
	// Field-level validation errors (Huma emits these for schema failures).
	if problem.Errors != nil {
		for _, e := range *problem.Errors {
			loc, m := "", ""
			if e.Location != nil {
				loc = *e.Location
			}
			if e.Message != nil {
				m = *e.Message
			}
			if loc != "" || m != "" {
				fmt.Fprintf(&msg, "\n  - %s: %s", loc, m)
			}
		}
	}
	if msg.Len() == 0 {
		return fmt.Errorf("%s", status)
	}
	return fmt.Errorf("%s", msg.String())
}

// timeLayout is the compact, sortable timestamp rendering for table cells.
const timeLayout = "2006-01-02 15:04:05"

// fmtTime renders an optional timestamp for a table cell; nil/zero renders "—".
func fmtTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return t.Format(timeLayout)
}

// deref returns *p or "" when nil — a terse helper for the many *string fields.
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// yesNo renders a bool as a compact table cell.
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
