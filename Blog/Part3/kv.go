//
// A more sophisticated / idempotent approach to writing data
// Standalone, nothing else needed

// ************************************************************

package main

import (
	"fmt"
	"os"
	"github.com/arangodb/go-driver/http"
	A "github.com/arangodb/go-driver"
)

// ************************************************************

type IntKeyValue struct {

	K  string `json:"_key"`
	V  int    `json:"value"`
}

// ************************************************************

func main() {

	fmt.Println("Create a key value lookup table")

	var dbname string = "SemanticSpacetime"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	db := OpenDatabase(dbname, service_url, user, pwd)

	// Create documents

	kv := []IntKeyValue{

		IntKeyValue{
			K: "NEAR",
			V:  1,
		},

		IntKeyValue{
			K: "FOLLOWS",
			V:  2,
		},

		IntKeyValue{
			K: "CONTAINS",
			V:  3,
		},

		IntKeyValue{
			K: "EXPRESSES",
			V:  4,
		},

	}

	// Add to DB

	SaveIntKVMap("ST_Types_Map",db,kv)

	// Retrieve from DB

	PrintIntKV(db,"ST_Types_Map")

	// Import constant lookup table from DB

	var const_STtype = make(map[string]int)

	LoadIntKV2Map(db,"ST_Types_Map", const_STtype)

	fmt.Println("RESULT 1 (corrected values): ",const_STtype)

	IncrementIntKV(db,"ST_Types_Map","EXPRESSES")
	LoadIntKV2Map(db,"ST_Types_Map", const_STtype)

	fmt.Println("RESULT 2 (incremented values): ",const_STtype)

	fmt.Println("Using const_STtype[\"CONTAINS\"] as a named constant/invariant: ",const_STtype["CONTAINS"])
}

//********************************************************
// Toolkit
//********************************************************

func OpenDatabase(name, url, user, pwd string) A.Database {

	// Simple encapsulation of opening an Arango DB

	var db A.Database
	var db_exists bool
	var err error
	var client A.Client

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ url },
	})

	if err != nil {
		fmt.Printf("Failed to create HTTP connection: %v", err)
		os.Exit(1)
	}

	client, err = A.NewClient(A.ClientConfig{
		Connection: conn,
		Authentication: A.BasicAuthentication(user, pwd),
	})

	db_exists, err = client.DatabaseExists(nil,name)

	if db_exists {

		db, err = client.Database(nil,name)

	} else {
		db, err = client.CreateDatabase(nil,name, nil)
		
		if err != nil {
			fmt.Printf("Failed to create database: %v", err)
			os.Exit(1);
		}
	}

	return db
}

//********************************************************

func SaveIntKVMap(collname string, db A.Database, kv []IntKeyValue) {

	// Create collection

	var err error
	var coll_exists bool
	var coll A.Collection

	coll_exists, err = db.CollectionExists(nil, collname)

	if coll_exists {
		fmt.Println("Collection " + collname +" exists already")

		coll, err = db.Collection(nil, collname)

		if err != nil {
			fmt.Printf("Existing collection: %v", err)
			os.Exit(1)
		}

	} else {

		coll, err = db.CreateCollection(nil, collname, nil)

		if err != nil {
			fmt.Printf("Failed to create collection: %v", err)
			os.Exit(1)
		}
	}

	for k := range kv {

		AddIntKV(coll, kv[k])
	}
}

// **************************************************

func PrintIntKV(db A.Database, coll_name string) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + coll_name +" LIMIT 10 RETURN doc"

	cursor,err = db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Query \""+ querystring +"\" failed: %v", err)
		return
	}

	defer cursor.Close()

	for {
		var kv IntKeyValue

		metadata,err := cursor.ReadDocument(nil,&kv)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("KV returned: %v", err)
		} else {
			
			fmt.Print("(K,V): (",kv.K,",", kv.V,")    ....    (",metadata,")\n")
		}
	}
}

// **************************************************

func AddIntKV(coll A.Collection, kv IntKeyValue) {

	// Add data with convergent semantics, CFEngine style

	exists,err := coll.DocumentExists(nil, kv.K)

	if !exists {

		fmt.Println("Adding/Restoring",kv)
		_, err = coll.CreateDocument(nil, kv)
		
		if err != nil {
			fmt.Printf("Failed to create non existent node: %s %v",kv.K,err)
			os.Exit(1);
		}
	} else {

		var checkkv IntKeyValue
		
		_,err = coll.ReadDocument(nil,kv.K,&checkkv)

		if checkkv.V != kv.V {
			fmt.Println("Correcting data",checkkv,"to",kv)
			_, err := coll.UpdateDocument(nil, kv.K, kv)
			if err != nil {
				fmt.Printf("Failed to update value: %s %v",kv.K,err)
				os.Exit(1);
			}
		}
	}
}

// **************************************************

func IncrementIntKV(db A.Database, coll_name, key string) {

        // UPDATE doc WITH { karma: doc.karma + 1 } IN users

	querystring := "LET doc = DOCUMENT(\"" + coll_name + "/" + key + "\")\nUPDATE doc WITH { value: doc.value + 1 } IN " + coll_name

	cursor,err := db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Query \""+ querystring +"\" failed: %v", err)
	}

	cursor.Close()
}

// **************************************************

func LoadIntKV2Map(db A.Database, coll_name string, extkv map[string]int) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + coll_name +" LIMIT 10 RETURN doc"

	cursor,err = db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
		os.Exit(1)
	}

	defer cursor.Close()

	for {
		var kv IntKeyValue

		_,err = cursor.ReadDocument(nil,&kv)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("KV returned: %v", err)
		} else {
			extkv[kv.K] = kv.V
		}
	}
}

