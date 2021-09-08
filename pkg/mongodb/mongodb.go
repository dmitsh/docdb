package mongodb

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
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

type Query struct {
	query  string
	filter interface{}
	opts   *options.FindOptions
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

func (db *DB) Disconnect() (err error) {
	err = db.client.Disconnect(db.ctx)
	db.cancel()
	return
}

func (db *DB) Populate(data []interface{}) error {
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

func (db *DB) RunQuery(q interface{}, token string) ([]interface{}, string, error) {
	query, ok := q.(*Query)
	if !ok {
		return nil, "", errors.Errorf("Unexpected query type %s", reflect.TypeOf(query).String())
	}
	//fmt.Printf("Query %#v\n", query.filter)
	skip, err := query.setSkip(token)
	if err != nil {
		return nil, "", err
	}
	return db.query(query, skip)
}

func (db *DB) query(query *Query, skip int) ([]interface{}, string, error) {
	cur, err := db.collection.Find(db.ctx, query.filter, []*options.FindOptions{query.opts}...)
	if err != nil {
		return nil, "", err
	}
	defer cur.Close(db.ctx)
	ret := []interface{}{}
	for cur.Next(db.ctx) {
		var result bson.M
		if err := cur.Decode(&result); err != nil {
			return nil, "", err
		}
		ret = append(ret, result)
	}
	if err := cur.Err(); err != nil {
		return nil, "", err
	}
	var token string
	if query.opts.Limit != nil && *query.opts.Limit != 0 {
		token = strconv.Itoa(skip + len(ret))
	}
	return ret, token, nil
}

func (query *Query) VisitEQ(f *queries.FilterEQ) (string, error) {
	// { <key>: <val> }
	return fmt.Sprintf("{ %q: %q }", f.Key, f.Val), nil
}

func (query *Query) VisitIN(f *queries.FilterIN) (string, error) {
	// { $in: [ <val1>, <val2>, ... , <valN> ] }
	if len(f.Vals) == 0 {
		return "", fmt.Errorf("empty IN operator for key %q", f.Key)
	}
	str := fmt.Sprintf(`{ %q: { "$in": [ %q`, f.Key, f.Vals[0])
	for _, v := range f.Vals[1:] {
		str += fmt.Sprintf(", %q", v)
	}
	str += " ] } }"
	return str, nil
}

func (query *Query) visitFilters(op string, filters []queries.Filter) (string, error) {
	arr := []string{}
	for _, filter := range filters {
		switch f := filter.(type) {
		case *queries.FilterEQ:
			if str, err := query.VisitEQ(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		case *queries.FilterIN:
			if str, err := query.VisitIN(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		case *queries.FilterOR:
			if str, err := query.VisitOR(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		case *queries.FilterAND:
			if str, err := query.VisitAND(f); err != nil {
				return "", err
			} else {
				arr = append(arr, str)
			}
		default:
			return "", fmt.Errorf("Unsupported filter type %#v", f)
		}
	}
	return fmt.Sprintf(`{ "%s": [ %s ] }`, op, strings.Join(arr, ", ")), nil
}

func (query *Query) VisitAND(f *queries.FilterAND) (string, error) {
	// { $and: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
	return query.visitFilters("$and", f.Filters)
}

func (query *Query) VisitOR(f *queries.FilterOR) (string, error) {
	// { $or: [ { <expression1> }, { <expression2> } , ... , { <expressionN> } ] }
	return query.visitFilters("$or", f.Filters)
}

func (query *Query) Finalize(filters string, mq *queries.MidQuery) error {
	query.query = filters
	if len(filters) == 0 {
		query.filter = bson.D{}
	} else if err := bson.UnmarshalExtJSON([]byte(filters), false, &query.filter); err != nil {
		return err
	}
	query.opts = options.Find()

	// sorting
	if len(mq.Sort) > 0 {
		sort := bson.D{}
		for _, s := range mq.Sort {
			order := 1 // ascending
			if s.Order == queries.DESC {
				order = -1
			}
			sort = append(sort, bson.E{Key: s.Key, Value: order})
		}
		query.opts.SetSort(sort)
	}
	// pagination
	if mq.Page.Limit > 0 {
		query.opts.SetLimit(int64(mq.Page.Limit))
	}
	if _, err := query.setSkip(mq.Page.Token); err != nil {
		return err
	}
	return nil
}

func (query *Query) setSkip(token string) (skip int, err error) {
	if len(token) != 0 {
		if skip, err = strconv.Atoi(token); err != nil {
			return
		}
		query.opts.SetSkip(int64(skip))
	}
	return
}
