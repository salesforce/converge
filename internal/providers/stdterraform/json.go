package stdterraform

import (
	"bytes"
	"encoding/json"
)

// decodeStrict unmarshals JSON into v, rejecting unknown fields so a typo'd or stale
// spec key (e.g. "sources" for "source") fails loudly at the resource rather than
// silently running an empty/partial config. Used only for the spec, which the
// provider owns; the effective config is decoded leniently by converge.EffectiveConfig.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
