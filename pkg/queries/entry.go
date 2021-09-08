package queries

type Person struct {
	Name string `json:"name,omitempty"`
	Org  string `json:"org,omitempty"`
	Code int    `json:"code,omitempty"`
}

type Entry struct {
	Person Person `json:"person,omitempty"`
	City   string `json:"city,omitempty"`
	State  string `json:"state,omitempty"`
}
