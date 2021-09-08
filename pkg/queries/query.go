package queries

import (
	"encoding/json"
	"fmt"
)

type SortingOrder int

const (
	FILTER = "filter"
	SORT   = "sort"
	PAGE   = "pagination"
	ASC    = "ASC"
	DESC   = "DESC"
)

type DbInterface interface {
	Populate([]interface{}) error
	RunQuery(interface{}, string) ([]interface{}, string, error)
	Disconnect() error
}

type Sorting struct {
	Key   string `json:"key"`
	Order string `json:"order,omitempty"`
}

type Pagination struct {
	Limit int    `json:"limit"`
	Token string `json:"token,omitempty"`
}

type MidQuery struct {
	Filters map[string]interface{} `json:"filter"`
	Sort    []Sorting              `json:"sort"`
	Page    Pagination             `json:"page"`

	// derived from Filters
	Filter Filter
}

func (q *MidQuery) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	if elem, ok := m[FILTER]; ok {
		q.Filter, err = parseFilter(elem)
		if err != nil {
			return err
		}
	}
	// setting sorting
	if elem, ok := m[SORT]; ok {
		arr, ok := elem.([]interface{})
		if !ok {
			return fmt.Errorf("%q must be an array", SORT)
		}
		jdata, err := json.Marshal(arr)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(jdata, &q.Sort); err != nil {
			return err
		}
	}
	// setting pagination
	if elem, ok := m[PAGE]; ok {
		page, ok := elem.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%q must be a map", PAGE)
		}
		jdata, err := json.Marshal(page)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(jdata, &q.Page); err != nil {
			return err
		}
	}
	return nil
}
