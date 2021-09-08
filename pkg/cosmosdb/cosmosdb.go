package cosmosdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/a8m/documentdb"
	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/pkg/errors"
)

type DB struct {
	url    string
	key    string
	dbName string
	cName  string

	client     *documentdb.DocumentDB
	collection *documentdb.Collection
}

type UserData interface{}

type DocData struct {
	documentdb.Document
	UserData
}

type Query struct {
	query documentdb.Query
	limit int
}

func GetDB(cfg map[string]string) (queries.DbInterface, error) {
	db := &DB{
		url:    cfg["url"],
		key:    cfg["key"],
		dbName: cfg["db"],
		cName:  cfg["container"],
	}
	db.client = documentdb.New(db.url, &documentdb.Config{
		MasterKey: &documentdb.Key{
			Key: db.key,
		},
	})
	dbs, err := db.client.QueryDatabases(&documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: db.dbName},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(dbs) == 0 {
		return nil, fmt.Errorf("database %s for CosmosDB state store not found", db.dbName)
	}

	dbc := &dbs[0]
	fmt.Printf("DB: %#v\n", dbc)
	colls, err := db.client.QueryCollections(dbc.Self, &documentdb.Query{
		Query: "SELECT * FROM ROOT r WHERE r.id=@id",
		Parameters: []documentdb.Parameter{
			{Name: "@id", Value: db.cName},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(colls) == 0 {
		return nil, fmt.Errorf("collection %s for CosmosDB state store not found. This must be created before Dapr uses it.", db.cName)
	}
	db.collection = &colls[0]
	return db, nil
}

func (db *DB) Populate(data []interface{}) error {
	for _, entry := range data {
		doc := &DocData{UserData: entry}
		jsonbytes, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		fmt.Println(string(jsonbytes))
		/*fmt.Printf("ADD %#v\n", doc)
		resp, err := client.UpsertDocument(colls.Self, &doc)
		if err != nil {
			return err
		}
		fmt.Printf("RESP: %#v\n", resp)*/
	}
	return nil
}

func (db *DB) RunQuery(q interface{}, token string) ([]interface{}, string, error) {
	query, ok := q.(*Query)
	if !ok {
		return nil, "", errors.Errorf("Unexpected query type %s", reflect.TypeOf(q).String())
	}
	opts := []documentdb.CallOption{documentdb.CrossPartition()}
	if query.limit != 0 {
		opts = append(opts, documentdb.Limit(query.limit))
	}
	if len(token) != 0 {
		opts = append(opts, documentdb.Continuation(token))
	}
	//fmt.Printf("QUERY: %#v\n", query.query)
	docs := []interface{}{}
	resp, err := db.client.QueryDocuments(db.collection.Self, &query.query, &docs, opts...)
	if err != nil {
		return nil, "", err
	}
	token = resp.Header.Get(documentdb.HeaderContinuation)
	return docs, token, nil
}

func (db *DB) Disconnect() error {
	return nil
}

func (q *Query) setNextParamter(val string) string {
	pname := fmt.Sprintf("@__param__%d__", len(q.query.Parameters))
	q.query.Parameters = append(q.query.Parameters, documentdb.Parameter{Name: pname, Value: val})
	return pname
}

func (q *Query) VisitEQ(f *queries.FilterEQ) (string, error) {
	// <key> = <val>
	val, ok := f.Val.(string)
	if !ok {
		return "", fmt.Errorf("unsupported type of value %#v; expected string", f.Val)
	}
	name := q.setNextParamter(val)
	return fmt.Sprintf("c.%s = %s", f.Key, name), nil
}

func (q *Query) VisitIN(f *queries.FilterIN) (string, error) {
	// <key> IN ( <val1>, <val2>, ... , <valN> )
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}
	names := make([]string, len(f.Vals))
	for i, v := range f.Vals {
		val, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("unsupported type of value %#v; expected string", v)
		}
		names[i] = q.setNextParamter(val)
	}
	return fmt.Sprintf("c.%s IN (%s)", f.Key, strings.Join(names, ", ")), nil
}

func (q *Query) visitFilters(op string, filters []queries.Filter) (string, error) {
	arr := []string{}
	for _, filter := range filters {
		switch f := filter.(type) {
		case *queries.FilterEQ:
			if str, err := q.VisitEQ(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		case *queries.FilterIN:
			if str, err := q.VisitIN(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		case *queries.FilterOR:
			if str, err := q.VisitOR(f); err != nil {
				return "", err
			} else {
				arr = append(arr, "("+str+")")
			}
		case *queries.FilterAND:
			if str, err := q.VisitAND(f); err != nil {
				return "", err
			} else {
				arr = append(arr, "("+str+")")
			}
		default:
			return "", fmt.Errorf("Unsupported filter type %#v", f)
		}
	}
	return strings.Join(arr, " "+op+" "), nil
}

func (q *Query) VisitAND(f *queries.FilterAND) (string, error) {
	// <expression1> AND <expression2> AND ... AND <expressionN>
	return q.visitFilters("AND", f.Filters)
}

func (q *Query) VisitOR(f *queries.FilterOR) (string, error) {
	// <expression1> OR <expression2> OR ... OR <expressionN>
	return q.visitFilters("OR", f.Filters)
}

func (q *Query) Finalize(filters string, mq *queries.MidQuery) error {
	var filter, orderBy string
	if len(filters) != 0 {
		filter = fmt.Sprintf(" WHERE %s", filters)
	}
	if sz := len(mq.Sort); sz != 0 {
		order := make([]string, sz)
		for i, item := range mq.Sort {
			if item.Order == queries.DESC {
				order[i] = fmt.Sprintf("c.%s DESC", item.Key)
			} else {
				order[i] = fmt.Sprintf("c.%s ASC", item.Key)
			}
		}
		orderBy = fmt.Sprintf(" ORDER BY %s", strings.Join(order, ", "))
	}
	q.query.Query = fmt.Sprintf("SELECT * FROM c%s%s", filter, orderBy)
	q.limit = mq.Page.Limit
	return nil
}
