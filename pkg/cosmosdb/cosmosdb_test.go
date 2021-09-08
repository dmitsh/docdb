package cosmosdb

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/a8m/documentdb"
	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/stretchr/testify/assert"
)

func TestSqlQuery(t *testing.T) {
	tests := []struct {
		input string
		query documentdb.Query
	}{
		{
			input: "../../tests/q1.json",
			query: documentdb.Query{
				Query:      "SELECT * FROM c",
				Parameters: nil,
			},
		},
		{
			input: "../../tests/q2.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.state = @__param__0__",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "CA",
					},
				},
			},
		},
		{
			input: "../../tests/q3.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.person.org = @__param__0__ AND c.state IN (@__param__1__, @__param__2__) ORDER BY c.state DESC, c.person.name ASC",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "A",
					},
					{
						Name:  "@__param__1__",
						Value: "CA",
					},
					{
						Name:  "@__param__2__",
						Value: "WA",
					},
				},
			},
		},
		{
			input: "../../tests/q4.json",
			query: documentdb.Query{
				Query: "SELECT * FROM c WHERE c.person.org = @__param__0__ OR (c.person.org = @__param__1__ AND c.state IN (@__param__2__, @__param__3__)) ORDER BY c.state DESC, c.person.name ASC",
				Parameters: []documentdb.Parameter{
					{
						Name:  "@__param__0__",
						Value: "A",
					},
					{
						Name:  "@__param__1__",
						Value: "B",
					},
					{
						Name:  "@__param__2__",
						Value: "CA",
					},
					{
						Name:  "@__param__3__",
						Value: "WA",
					},
				},
			},
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
