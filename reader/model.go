package reader

// TemplateKey holds the template keys
type TemplateKey struct {
	Key  string `json:"key"`
	Kind string `json:"kind"`
	Tag  string `json:"tag"`
}

// Template holds the template config
type Template struct {
	Keys []TemplateKey `json:"data"`
}
