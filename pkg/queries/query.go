package queries

import (
	"encoding/json"
	"sort"

	"github.com/pkg/errors"
)

const (
	FILTER = "filter"
	OR     = "$or"
)

type DbInterface interface {
	Populate([]Entry) error
	GetAll() error
	ToQuery(*MidQuery) (interface{}, error)
	RunQuery(interface{}) (interface{}, error)
}

type MidQuery struct {
	Filter map[string]interface{}
}

func CreateQuery(txt string) (*MidQuery, error) {
	m := make(map[string]interface{})
	if err := json.Unmarshal([]byte(txt), &m); err != nil {
		return nil, errors.Wrap(err, "Error parsing query")
	}
	query := &MidQuery{}
	if filter, ok := m[FILTER]; ok {
		query.Filter, ok = filter.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("'%s' must be a map", FILTER)
		}
	}
	return query, nil
}

func (mq *MidQuery) SortedKeys() []string {
	keys := make([]string, len(mq.Filter))
	i := 0
	for key := range mq.Filter {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}
