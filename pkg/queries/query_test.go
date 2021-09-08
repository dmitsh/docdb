package queries

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		input string
		query MidQuery
	}{
		{
			input: "../../tests/q2.json",
			query: MidQuery{
				Filters: nil,
				Sort:    nil,
				Page:    Pagination{Limit: 2, Token: ""},
				Filter:  &FilterEQ{Key: "state", Val: "CA"},
			},
		},
		{
			input: "../../tests/q3.json",
			query: MidQuery{
				Filters: nil,
				Sort: []Sorting{
					{Key: "state", Order: "DESC"},
					{Key: "person.name", Order: ""},
				},
				Page: Pagination{Limit: 0, Token: ""},
				Filter: &FilterAND{
					Filters: []Filter{
						&FilterEQ{Key: "person.org", Val: "A"},
						&FilterIN{Key: "state", Vals: []interface{}{"CA", "WA"}},
					},
				},
			},
		},
		{
			input: "../../tests/q4.json",
			query: MidQuery{
				Filters: nil,
				Sort: []Sorting{
					{Key: "state", Order: "DESC"},
					{Key: "person.name", Order: ""},
				},
				Page: Pagination{Limit: 2, Token: ""},
				Filter: &FilterOR{
					Filters: []Filter{
						&FilterEQ{Key: "person.org", Val: "A"},
						&FilterAND{
							Filters: []Filter{
								&FilterEQ{Key: "person.org", Val: "B"},
								&FilterIN{Key: "state", Vals: []interface{}{"CA", "WA"}},
							},
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var mq MidQuery
		err = json.Unmarshal(data, &mq)
		assert.NoError(t, err)
		assert.Equal(t, test.query, mq)
	}
}
