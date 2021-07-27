//
// Bare bones Go(lang) program to create a graph
//

package main

import (
	"fmt"
	"os"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

// **********************************************************

type NodeDataType struct {
	Name string `json:"_key"`
	Age  int    `json:"age"`
}

type LinkDataType struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}

// **********************************************************

func main() {
	fmt.Println("Hello World")

	// Create an HTTP connection to the database

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})

	if err != nil {
		fmt.Printf("Failed to create HTTP connection: %v", err)
		os.Exit(1)
	}

	// Create a client

	c, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
		Authentication: driver.BasicAuthentication("root", "mark"),
	})

	// Create database

	db, err := c.CreateDatabase(nil, "my_test_graph_database", nil)

	if err != nil {
		fmt.Printf("Failed to create database: %v", err)
		os.Exit(1)
	}

	// define the edgeCollection to store the edges

	var edgeDefinition driver.EdgeDefinition
	edgeDefinition.Collection = "Links"

	// define a set of collections where an edge is going out...

	edgeDefinition.From = []string{"Nodes"}

	// repeat this for the collections where an edge is going into

	edgeDefinition.To = []string{"Nodes"}

	// A graph can contain additional vertex collections, defined in the set of orphan collections
	
	var options driver.CreateGraphOptions

	//options.OrphanVertexCollections = []string{"myCollection4", "myCollection5"}

	options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

	// now it's possible to create a graph

	graph, err := db.CreateGraph(nil, "myGraph", &options)

	if err != nil {
		fmt.Printf("Failed to create graph: %v", err)
		os.Exit(1)
	}

	// add vertex / node

	vertexCollection1, err := graph.VertexCollection(nil, "Nodes")

	if err != nil {
		fmt.Printf("Failed to get vertex collection: %v", err)
		os.Exit(1)
	}

	// Initialize a list of objects

	myObjects := []NodeDataType{

		NodeDataType{
			"Node1",
			38,
		},

		NodeDataType{
			"Node2",
			36,
		},
	}
	_, _, err = vertexCollection1.CreateDocuments(nil, myObjects)

	if err != nil {
		fmt.Printf("Failed to create vertex documents: %v", err)
		os.Exit(1)
	}

	// add edge / link

	edgeCollection, _, err := graph.EdgeCollection(nil, "Links")

	if err != nil {
		fmt.Printf("Failed to select edge collection: %v", err)
		os.Exit(1)
	}

	edge := LinkDataType{From: "Nodes/Node1", To: "Nodes/Node2"}

	_, err = edgeCollection.CreateDocument(nil, edge)

	if err != nil {
		fmt.Printf("Failed to create edge document: %v", err)
		os.Exit(1)
	}

	// delete graph
	// graph.Remove(nil)
}
