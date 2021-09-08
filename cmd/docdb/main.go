package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/dmitsh/docdb/pkg/cosmosdb"
	"github.com/dmitsh/docdb/pkg/mongodb"
	"github.com/dmitsh/docdb/pkg/queries"
	"github.com/pkg/errors"
)

type Person struct {
	Name string `json:"name,omitempty"`
	Org  string `json:"org,omitempty"`
	Code int    `json:"code,omitempty"`
}

type Entry struct {
	Person Person `json:"person,omitempty"`
	City   string `json:"city,omitempty"`
	State  string `json:"state,omitempty"`
}

func main() {
	if err := run(); err != nil {
		fmt.Println("ERROR:", err.Error())
		os.Exit(1)
	}
	fmt.Println("OK")
}

func run() error {
	var (
		cfile, ifile, qfile string
		db                  queries.DbInterface
		visitor             queries.Visitor
	)
	flag.StringVar(&cfile, "c", "", "DB config filepath")
	flag.StringVar(&ifile, "i", "", "input data filepath")
	flag.StringVar(&qfile, "q", "", "query filepath")
	flag.Parse()

	// read config
	config, err := getConfig(cfile)
	if err != nil {
		return err
	}

	switch config["type"] {
	case "cosmosdb":
		visitor = &cosmosdb.Query{}
		db, err = cosmosdb.GetDB(config)
	case "mongodb":
		visitor = &mongodb.Query{}
		db, err = mongodb.GetDB(config)
	default:
		err = errors.Errorf("Unsupported DB type %q", config["type"])
	}
	if err != nil {
		return err
	}
	defer db.Disconnect()

	switch {
	case len(ifile) != 0:
		data, err := getData(ifile)
		if err != nil {
			return err
		}
		return db.Populate(data)

	case len(qfile) != 0:
		return processQuery(qfile, db, visitor)
	}
	return nil
}

func getConfig(fname string) (map[string]string, error) {
	if len(fname) == 0 {
		return nil, errors.Errorf("Missing config file")
	}
	content, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	config := make(map[string]string)
	err = json.Unmarshal(content, &config)
	return config, err
}

func getData(fname string) ([]interface{}, error) {
	content, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	data := []interface{}{}
	err = json.Unmarshal(content, &data)
	return data, err
}

func processQuery(fname string, db queries.DbInterface, visitor queries.Visitor) error {
	data, err := os.ReadFile(fname)
	if err != nil {
		return err
	}
	var mq queries.MidQuery
	if err = json.Unmarshal(data, &mq); err != nil {
		return err
	}
	//
	qbuilder := queries.NewQueryBuilder(visitor)
	err = qbuilder.BuildQuery(&mq)

	if err != nil {
		return err
	}
	var (
		ret   []interface{}
		token string
	)
	for {
		fmt.Println("RUN QUERY")
		ret, token, err = db.RunQuery(visitor, token)
		if err != nil {
			return errors.Wrap(err, "processQuery")
		}
		for _, item := range ret {
			printItem(item)
		}
		if len(ret) == 0 || len(token) == 0 {
			fmt.Println("EOF")
			break
		}
	}
	return nil
}

func printItem(data interface{}) {
	jdata, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Parsing error %v\n", err)
		return
	}
	var obj Entry
	if err = json.Unmarshal(jdata, &obj); err != nil {
		fmt.Println(string(jdata))
	} else {
		fmt.Printf("%#v\n", obj)
	}
}
