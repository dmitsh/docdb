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

type DocData struct {
	documentdb.Document
	queries.Entry
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

func (db *DB) Populate(data []queries.Entry) error {
	for _, entry := range data {
		doc := &DocData{Entry: entry}
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

func (db *DB) GetAll() error {
	docs := []queries.Entry{}
	resp, err := db.client.ReadDocuments(db.collection.Self, &docs)
	if err != nil {
		return err
	}
	fmt.Printf("RESP: %#v\n", resp)
	for _, doc := range docs {
		jsonbytes, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		fmt.Println(string(jsonbytes))
	}
	return nil
}

func (db *DB) ToQuery(mq *queries.MidQuery) (interface{}, error) {
	filters := []string{}
	params := []documentdb.Parameter{}
	keys := mq.SortedKeys()
	for _, key := range keys {
		val := mq.Filter[key]
		name := "@" + strings.ReplaceAll(key, ".", "__")
		switch v := val.(type) {
		case string:
			filters = append(filters, fmt.Sprintf("c.%s = %s", key, name))
			params = append(params, documentdb.Parameter{Name: name, Value: v})
		case []interface{}:
			names := make([]string, len(v))
			for i := range v {
				names[i] = fmt.Sprintf("%s__%d", name, i)
				params = append(params, documentdb.Parameter{Name: names[i], Value: v[i].(string)})
			}
			filters = append(filters, fmt.Sprintf("c.%s IN (%s)", key, strings.Join(names, ", ")))
		default:
			return nil, fmt.Errorf("ERR %#v", val)
		}
	}
	return &documentdb.Query{
		Query:      fmt.Sprintf("SELECT * FROM c WHERE %s", strings.Join(filters, " AND ")),
		Parameters: params,
	}, nil
}

func (db *DB) RunQuery(q interface{}) (interface{}, error) {
	query, ok := q.(*documentdb.Query)
	if !ok {
		return nil, errors.Errorf("Unexpected query type %s; expected *documentdb.Query", reflect.TypeOf(q).String())
	}
	docs := []queries.Entry{}
	fmt.Printf("QUERY: %#v\n", query)
	_, err := db.client.QueryDocuments(db.collection.Self, query, &docs, documentdb.CrossPartition())
	if err != nil {
		return nil, err
	}
	for _, doc := range docs {
		jsonbytes, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		fmt.Println(string(jsonbytes))
	}
	return nil, nil
}
