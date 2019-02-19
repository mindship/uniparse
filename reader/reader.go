package reader

import (
	"context"
	"encoding/json"
)

// Reader is the interface for converting JSON template to struct
type Reader interface {
	FromJSON(ctx context.Context, jsonTemplate string) (Template, error)
}

type reader struct{}

// FromJSON converts JSON string to the template model
func (r *reader) FromJSON(ctx context.Context, jsonTemplate string) (Template, error) {
	var template Template

	err := json.Unmarshal([]byte(jsonTemplate), &template.Keys)
	if err != nil {
		return template, err
	}

	return template, nil
}

// NewReader is the initialization method for template
func NewReader() Reader {
	return &reader{}
}
