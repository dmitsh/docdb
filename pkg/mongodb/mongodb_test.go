package mongodb

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/stretchr/testify/assert"
)

func TestMongoQuery(t *testing.T) {
	tests := []struct {
		input string
		query string
	}{
		{
			input: "../../tests/q1.json",
			query: ``,
		},
		{
			input: "../../tests/q2.json",
			query: `{ "state": "CA" }`,
		},
		{
			input: "../../tests/q3.json",
			query: `{ "$and": [ { "person.org": "A" }, { "state": { "$in": [ "CA", "WA" ] } } ] }`,
		},
		{
			input: "../../tests/q4.json",
			query: `{ "$or": [ { "person.org": "A" }, { "$and": [ { "person.org": "B" }, { "state": { "$in": [ "CA", "WA" ] } } ] } ] }`,
		},
	}
	for _, test := range tests {
		data, err := os.ReadFile(test.input)
		assert.NoError(t, err)
		var mq queries.MidQuery
		err = json.Unmarshal(data, &mq)
		assert.NoError(t, err)

		query := &Query{}
		qbuilder := queries.NewQueryBuilder(query)
		err = qbuilder.BuildQuery(&mq)
		assert.NoError(t, err)
		assert.Equal(t, test.query, query.query)
	}
}
