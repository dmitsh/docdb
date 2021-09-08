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

func main() {
	var (
		cfile, ifile, qfile string
		getall              bool
		db                  queries.DbInterface
		err                 error
	)
	flag.StringVar(&cfile, "c", "", "DB config filepath")
	flag.StringVar(&ifile, "i", "", "input data filepath")
	flag.BoolVar(&getall, "a", false, "get all data")
	flag.StringVar(&qfile, "q", "", "query filepath")
	flag.Parse()

	// read config
	config, err := getConfig(cfile)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	switch config["type"] {
	case "cosmosdb":
		db, err = cosmosdb.GetDB(config)
	case "mongodb":
		db, err = mongodb.GetDB(config)
	default:
		err = errors.Errorf("Unsupported DB type %q", config["type"])
	}
	if err != nil {
		fmt.Println("ERROR:", err.Error())
		os.Exit(1)
	}

	switch {
	case len(ifile) != 0:
		data, err := getData(ifile)
		if err != nil {
			break
		}
		err = db.Populate(data)

	case getall:
		err = db.GetAll()

	case len(qfile) != 0:
		txt, err := os.ReadFile(qfile)
		if err != nil {
			break
		}
		mq, err := queries.CreateQuery(string(txt))
		if err != nil {
			break
		}
		q, err := db.ToQuery(mq)
		if err != nil {
			break
		}
		_, err = db.RunQuery(q)
	}

	if err != nil {
		fmt.Println("ERROR:", err.Error())
	} else {
		fmt.Println("OK")
	}
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

func getData(fname string) ([]queries.Entry, error) {
	content, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	data := []queries.Entry{}
	err = json.Unmarshal(content, &data)
	return data, err
}
