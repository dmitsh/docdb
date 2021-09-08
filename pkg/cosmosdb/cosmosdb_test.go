package cosmosdb

import (
	"testing"

	"github.com/a8m/documentdb"
	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/stretchr/testify/assert"
)

func TestSqlQuery(t *testing.T) {
	tests := []struct {
		input string
		query *documentdb.Query
	}{
		{
			input: `
			{
				"filter": {
					"state": "CA"
				}
			}			
			`,
			query: &documentdb.Query{
				Query: "SELECT * FROM c WHERE c.state = @state",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@state",
						Value: "CA",
					},
				},
			},
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
			query: &documentdb.Query{
				Query: "SELECT * FROM c WHERE c.person.org = @person__org AND c.state IN (@state__0, @state__1)",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@person__org",
						Value: "Dev Ops",
					},
					{
						Name:  "@state__0",
						Value: "CA",
					},
					{
						Name:  "@state__1",
						Value: "WA",
					},
				},
			},
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
