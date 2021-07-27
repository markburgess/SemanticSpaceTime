
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"context"

	A "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {

	ctx := context.Background()
	var err error
	var client A.Client
	var conn A.Connection

	flag.Parse()

	conn, err = http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
		//Endpoints: []string{"https://5a812333269f.arangodb.cloud:8529/"},
	})
	if err != nil {
		log.Fatalf("Failed to create HTTP connection: %v", err)
	}
	client, err = A.NewClient(A.ClientConfig{
		Connection: conn,
		Authentication: A.BasicAuthentication("root", "mark"),
		//Authentication: A.BasicAuthentication("root", "wnbGnPpCXHwbP"),
	})

	var db A.Database
	var db_exists, coll_exists bool

	db_exists, err = client.DatabaseExists(ctx,"example")

	if db_exists {
		fmt.Println("That db exists already")

		db, err = client.Database(ctx,"example")

		if err != nil {
			log.Fatalf("Failed to open existing database: %v", err)
		}

	} else {
		db, err = client.CreateDatabase(ctx, "example", nil)
		
		if err != nil {
			log.Fatalf("Failed to create database: %v", err)
		}
	}

	// Create collection

	coll_exists, err = db.CollectionExists(ctx, "users")

	if coll_exists {
		fmt.Println("That collection exists already")
		PrintCollection(ctx,db,"users")

	} else {

		var col A.Collection
		col, err = db.CreateCollection(ctx, "users", nil)

		if err != nil {
			log.Fatalf("Failed to create collection: %v", err)
		}

		// Create documents
		users := []User{
			User{
				Name: "John",
				Age:  65,
			},
			User{
				Name: "Tina",
				Age:  25,
			},
			User{
				Name: "George",
				Age:  31,
			},
		}
		metas, errs, err := col.CreateDocuments(nil, users)

		if err != nil {
			log.Fatalf("Failed to create documents: %v", err)
		} else if err := errs.FirstNonNil(); err != nil {
			log.Fatalf("Failed to create documents: first error: %v", err)
		}

		fmt.Printf("Created documents with keys '%s' in collection '%s' in database '%s'\n", strings.Join(metas.Keys(), ","), col.Name(), db.Name())
	}
}

// **************************************************

func PrintCollection(ctx context.Context, db A.Database, name string) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN users LIMIT 10 RETURN doc"

	cursor,err = db.Query(ctx,querystring,nil)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var doc User
		var metadata A.DocumentMeta

		metadata,err = cursor.ReadDocument(ctx,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("Doc returned: %v", err)
		} else {
			fmt.Print("Dot doc ",metadata,doc,"\n")
		}
	}
}
