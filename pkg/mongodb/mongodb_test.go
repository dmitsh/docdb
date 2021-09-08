package mongodb

import (
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
			input: `
			{
				"filter": {
					"state": "CA"
				}
			}			
			`,
			query: `{ "state": "CA" }`,
		},
		{
			input: `
			{
				"filter": {
					"state": ["CA", "WA"],
					"person.org" : "Dev Ops"
				}
			}
			`,
			query: `{ "person.org": "Dev Ops", "state": { "$in": [ "CA", "WA" ] } }`,
		},
	}
	db := DB{}
	for _, test := range tests {
		mq, err := queries.CreateQuery(test.input)
		assert.NoError(t, err)
		query, err := db.ToQuery(mq)
		assert.NoError(t, err)
		assert.Equal(t, test.query, query)
	}
}
