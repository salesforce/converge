package stdshell

import (
	"bytes"
	"encoding/json"
)

// decodeStrict unmarshals JSON into v, rejecting unknown fields so a typo'd or
// stale spec key (e.g. "scripts" for "script") fails loudly at the resource rather
// than silently running an empty/partial config. Used only for the spec, which the
// provider owns; the effective config is decoded leniently by converge.EffectiveConfig.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// marshal is json.Marshal, wrapped so the status-encoding call site reads clearly
// and stays symmetric with decodeStrict.
func marshal(v any) ([]byte, error) { return json.Marshal(v) }
