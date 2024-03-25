package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/surrealdb/surrealdb.go"
)

type ColumnData struct {
	name   string
	type_  string
	ignore bool
	id     bool
}

func main() {

	data_path := flag.String("data", "", "")
	header_path := flag.String("header_path", "", "")
	server_addr := flag.String("host", "127.0.0.1", "")
	port := flag.Int("port", 8000, "")
	namespace := flag.String("namespace", "test", "")
	database := flag.String("database", "test", "")
	table_name := flag.String("table", "", "")

	flag.Parse()

	fmt.Println("Reading data from", *data_path)
	fmt.Println("Headers are in", *header_path)

	data_file, err := os.Open(*data_path)

	if err != nil {
		log.Fatal("Dang file wouldn't open", err)
	}

	defer data_file.Close()

	header_file, err := os.Open(*header_path)

	if err != nil {
		log.Fatal("Cannot reader headers", err)
	}

	defer header_file.Close()

	header_reader := csv.NewReader(header_file)
	headers, err := header_reader.Read()
	if err != nil {
		log.Fatal("Cannot read headers", err)
	}

	data_reader := csv.NewReader(data_file)
	records, err := data_reader.ReadAll()

	if err != nil {
		fmt.Println("dang records", err)
	}

	conn_string := fmt.Sprintf("ws://%s:%d/rpc", *server_addr, *port)
	db, err := surrealdb.New(conn_string)
	if err != nil {
		log.Fatal("Could not connect to SurrealDB", err)
	}

	if _, err = db.Use(*namespace, *database); err != nil {
		log.Fatal("Invalid namespace/database", err)
	}

	column_data := make([]ColumnData, len(headers))
	for i, h := range headers {
		tokens := strings.Split(h, ":")
		name := tokens[0]
		type_ := tokens[1]
		ignore := type_ == "IGNORE"
		id := name == "ID"

		column_data[i] = ColumnData{name, type_, ignore, id}
	}

	for _, record := range records {
		id_assigned := false
		thing := *table_name
		data := make(map[string]interface{})

		for i, cd := range column_data {
			switch cd.type_ {
			case "ID":
				fmt.Println("ID")
				if !id_assigned {
					id_assigned = true
					thing += ":" + record[i]
				}
			case "IGNORE":
				fmt.Println("IGNORE")
			case "int":
				fmt.Println("int")
				if val, err := strconv.ParseInt(record[i], 10, 64); err != nil {
				} else {
					data[cd.name] = val
				}
			case "float":
				fmt.Println("float")
				if val, err := strconv.ParseFloat(record[i], 64); err != nil {
				} else {
					data[cd.name] = val
				}
			case "bool":
				fmt.Println("bool")
				if val, err := strconv.ParseBool(record[i]); err != nil {
				} else {
					data[cd.name] = val
				}
			default:
				fmt.Println("default")
				data[cd.name] = record[i]
			}
		}
		if _, err = db.Create(thing, data); err != nil {
			fmt.Printf("Could not create record %v with data %v\n Reason: %v\n", thing, data, err)
		}
	}
}
