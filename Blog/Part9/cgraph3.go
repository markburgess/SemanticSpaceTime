

package main

import (
	"fmt"
	"os"
	S "SST"
//	A "github.com/arangodb/go-driver"
)

// ********************************************************************************

func main() {

	var dbname string = "multiply"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	nodes := []S.Node{

		S.Node{
			"node1",
			"Satellite 1",
			"nodes",
			1.0,
		},

		S.Node{
			"node2",
			"Stellite 2",
			"nodes",
			1.0,
		},

		S.Node{
			"node3",
			"Satellite 3",
			"nodes",
			1.0,
		},

		S.Node{
			"node4",
			"Satellite 4",
			"nodes",
			1.0,
		},

		S.Node{
			"node5",
			"Satellite 5",
			"nodes",
			1.0,
		},
	}

	links := []S.Link{

		S.Link { 
			From: "Nodes/node1", 
			To: "Nodes/node2",
			SId: "CONNECTED",
			Weight: 2,           // Initialize all the same, positive 1
		},

		S.Link { 
			From: "Nodes/node2", 
			To: "Nodes/node3",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 2
		},
		S.Link { 
			From: "Nodes/node3", 
			To: "Nodes/node4",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 3
		},
		S.Link { 
			From: "Nodes/node4", 
			To: "Nodes/node1",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 4
		},
		S.Link { 
			From: "Nodes/node1", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 5
		},
		S.Link { 
			From: "Nodes/node2", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 6
		},
		S.Link { 
			From: "Nodes/node3", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 7
		},
		S.Link { 
			From: "Nodes/node4", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive 8
		},


	}

	// Convergent additions

	fmt.Println("========== NEW IDEMP FNs ==============")

	for node := range nodes {
		S.AddNode(g,nodes[node])
	}
	
	for link := range links {
		S.AddLink(g,links[link])
	}
	
	fmt.Println("========== SHOW MULTIPLY ==============")
	
	// Add a temporary subgraph

	S.AddNodeCollection(g, "NodeTmpFrom")
	S.AddNodeCollection(g, "NodeTmpTwo")
	S.AddNodeCollection(g, "NodeTmpThree")
	S.AddNodeCollection(g, "NodeTmpFinal")

	S.AddLinkCollection(g, "EdgeMatrix", "NodeTmpFrom") // needs nodevec to build allowed edges

	var symmetrized bool = true

	ExtractMatrix(g, "CONNECTED", "EdgeMatrix", symmetrized)
	ExtractVector(g, "EdgeMatrix", "NodeTmpFrom")

	// Copy over the subgraph node-weights from the source

	CopyWeights(g,"Nodes","NodeTmpFrom")

	S.PrintNodes(g, "NodeTmpFrom")

	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpFrom", "NodeTmpTwo")

	S.PrintNodes(g, "NodeTmpTwo")

	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpTwo", "NodeTmpThree")

	S.PrintNodes(g, "NodeTmpThree")

	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpThree", "NodeTmpFinal")

	S.PrintNodes(g, "NodeTmpFinal")

	fmt.Println("Normalize the result: divide all by sum = 242 --> (0.17,0.17,0.23,0.21,0.23)")
}

// ***************************************************************

func ExtractMatrix(g S.Analytics, assoc, matrix string, symmetrize bool) {

	var collection string
	var err error

	source := S.ASSOCIATIONS[assoc].STtype

	switch source {

	case S.GR_FOLLOWS:   collection = "Follows"
	case S.GR_CONTAINS:  collection = "Contains"
	case S.GR_EXPRESSES: collection = "Expresses"
	case S.GR_NEAR:      collection = "Near"

	default:           fmt.Println("MultiplyTempValues - no such association",source,assoc)
		           os.Exit(1)
	}

	// Clear existing documents/links for a clean slate

	clear := "FOR doc IN " + matrix + " REMOVE { _key: doc._key } IN " + matrix

	_, err = g.S_db.Query(nil,clear,nil)

	if err != nil {
		fmt.Printf("clearing failed in ExtractMatrix: %v", err)
	}

	// Copy matrix to a temporary directory so that we can optimize
	// Links first, use PARSE to strip off old Nodes/ qualifier and CONCAT to add new
	// This is a complex query!

	prefix := matrix + "/"

	copylinks := "FOR lnk IN " + collection + " FILTER lnk.semantics == \"" + assoc + "\" INSERT {  _from: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._from).key), _to: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._to).key), weight: lnk.weight } INTO " + matrix

	_, err = g.S_db.Query(nil,copylinks,nil)

	if err != nil {
		fmt.Printf("copylinks failed: %v", err)
	}

	// Now symmetrize the matrix

	if symmetrize {

		fmt.Println("Symmetrizing matrix")

		copylinks_symm := "FOR lnk IN " + collection + " FILTER lnk.semantics == \"" + assoc + "\" INSERT {  _from: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._to).key), _to: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._from).key), weight: lnk.weight } INTO " + matrix
		
		_, err = g.S_db.Query(nil,copylinks_symm,nil)
		
		if err != nil {
			fmt.Printf("copylinks failed: %v", err)
		}
	}
}

// ***************************************************************

func ExtractVector(g S.Analytics, matrix, vector string) {

	// Now create just the relevant nodes - idempotently with UPSERT (complex!)
	// confusingly, _id is collection/key qualfied, while _key is not
	// Edge nodes from/to need the qualifiers so use ID, nodes don't need CONCAT
	
	// Merge _from and _to references into a single list using UPSERT, INSERT, UPDATE
	
	gennodes_from := "FOR my IN " + matrix + " UPSERT { _key: PARSE_IDENTIFIER(my._from).key } INSERT { _key: PARSE_IDENTIFIER(my._from).key, weight: my.weight} UPDATE { weight: my.weight} INTO " + vector

	_, err := g.S_db.Query(nil,gennodes_from,nil)

	// Transpose matrix from reverse links, whether symmetrized or not

	gennodes_to := "FOR my IN " + matrix + " UPSERT { _key: PARSE_IDENTIFIER(my._to).key } INSERT { _key: PARSE_IDENTIFIER(my._to).key, weight: my.weight} UPDATE { weight: my.weight} INTO " + vector

	_, err = g.S_db.Query(nil,gennodes_to,nil)

	if err != nil {
		
		fmt.Println(err)
	}
}

// ***************************************************************

func CopyWeights(g S.Analytics, from, to string) {

	fmt.Println("Copying weights")
	
	copynodes := "FOR dest IN " + to + " FOR src IN " + from + " FILTER src._key == dest._key UPSERT { _key: src._key } INSERT { _key: src._key, weight: src.weight} UPDATE { weight: src.weight} INTO " + to

	_, err := g.S_db.Query(nil,copynodes,nil)

	if err != nil {
		
		fmt.Println(err)
	}
}

// ***************************************************************

func OperateMatrixOnVector(g S.Analytics, matrix, fromnodes, tonodes string) {

	// At this stage, we should have a copy of the matrix in EdgeMatrix
	// and a copy of the node vector in NodeTmpFrom, so multiply and store in NodeTmpTo

	// zero tonodes

	fmt.Println("Zeroing weights in ",tonodes)

	zero := "FOR doc IN " + tonodes + " REMOVE { _key: doc._key } IN " + tonodes

	_, err := g.S_db.Query(nil,zero,nil)

	if err != nil {
		
		fmt.Println(err)
	}

	multiply := "FOR link IN " + matrix + 
	" LET row = PARSE_IDENTIFIER(link._from) LET col = PARSE_IDENTIFIER(link._to) " + 
	"FOR vec IN " + fromnodes + " FILTER col.key == vec._key " + 
	"UPSERT { _key: row.key }" + 
	"INSERT { _key: row.key, weight: link.weight * vec.weight, comment: CONCAT(link.weight,\"*\",vec.weight,\"+\") } " + 
	"UPDATE { weight: OLD.weight + link.weight * vec.weight, comment: CONCAT(OLD.comment,link.weight,\"*\",vec.weight,\"+\") } INTO " + tonodes

	// LET current = append to list and sum it to get the -> to value

	_, err = g.S_db.Query(nil,multiply,nil)

	if err != nil {
		
		fmt.Println(err)
	}
}

