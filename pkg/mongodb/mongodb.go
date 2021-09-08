package mongodb

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const ()

type DB struct {
	url    string
	dbName string
	cName  string

	client     *mongo.Client
	collection *mongo.Collection

	ctx    context.Context
	cancel context.CancelFunc
}

func GetDB(cfg map[string]string) (queries.DbInterface, error) {
	var err error

	db := &DB{
		url:    cfg["url"],
		dbName: cfg["db"],
		cName:  cfg["collection"],
	}

	db.ctx, db.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	db.client, err = mongo.Connect(db.ctx, options.Client().ApplyURI(db.url))
	if err != nil {
		return nil, err
	}

	if err = db.client.Ping(db.ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	db.collection = db.client.Database(db.dbName).Collection(db.cName)
	if db.collection == nil {
		return nil, fmt.Errorf("No collection %s in DB %s", db.cName, db.dbName)
	}

	return db, nil
}

func (db *DB) disconnect() {
	db.client.Disconnect(db.ctx)
	db.cancel()
}

func (db *DB) Populate(data []queries.Entry) error {
	defer db.disconnect()

	for _, dat := range data {
		fmt.Printf("ADD %#v\n", dat)
		res, err := db.collection.InsertOne(db.ctx, dat)
		if err != nil {
			return err
		}
		fmt.Printf("RES: %#v\n", res)
	}
	return nil
}

func (db *DB) GetAll() error {
	return db.query(bson.D{})
}

func (db *DB) ToQuery(mq *queries.MidQuery) (interface{}, error) {
	filters := []string{}
	// sort map keys for testing consistency
	keys := mq.SortedKeys()

	for _, key := range keys {
		val := mq.Filter[key]
		switch v := val.(type) {
		case string:
			filters = append(filters, fmt.Sprintf("%q: %q", key, v))
		case []interface{}:
			f := fmt.Sprintf("%q: { %q: [ %q", key, "$in", v[0])
			for _, n := range v[1:] {
				f += fmt.Sprintf(", %q", n)
			}
			f += " ] }"
			filters = append(filters, f)
		default:
			return fmt.Sprintf("ERR %#v", val), nil
		}
	}
	return fmt.Sprintf("{ %s }", strings.Join(filters, ", ")), nil
}

func (db *DB) RunQuery(q interface{}) (interface{}, error) {
	var filter interface{}
	txt, ok := q.(string)
	if !ok {
		return nil, errors.Errorf("Unexpected query type %s; expected string", reflect.TypeOf(q).String())
	}
	err := bson.UnmarshalExtJSON([]byte(txt), false, &filter)
	//err := json.Unmarshal([]byte(txt), &filter)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Query %#v\n", filter)
	return nil, db.query(filter)
}

func (db *DB) query(filter interface{}) error {
	cur, err := db.collection.Find(db.ctx, filter)
	if err != nil {
		return err
	}
	defer cur.Close(db.ctx)

	for cur.Next(db.ctx) {
		//var result bson.D
		var result queries.Entry
		if err := cur.Decode(&result); err != nil {
			return err
		}
		fmt.Printf("%#v\n", result)
	}
	if err := cur.Err(); err != nil {
		return err
	}
	return nil
}

func (db *DB) query1(filter interface{}) error {
	var result queries.Entry
	err := db.collection.FindOne(db.ctx, filter).Decode(&result)
	if err == nil {
		fmt.Printf("%#v\n", result)
		return nil
	}
	if err == mongo.ErrNoDocuments {
		fmt.Println("record does not exist")
		return nil
	}
	return err
}
