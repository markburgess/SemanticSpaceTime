

package main

import (
	"fmt"
	S "SST"
//	A "github.com/arangodb/go-driver"
)

// ********************************************************************************

func main() {

	var dbname string = "SemanticSpacetime"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	S.AddNodeCollection(g, "NodeTmpFrom")
	S.AddNodeCollection(g, "NodeTmpTwo")
	S.AddNodeCollection(g, "NodeTmpThree")
	S.AddNodeCollection(g, "NodeTmpFinal")

	S.AddLinkCollection(g, "EdgeMatrix", "NodeTmpFrom") // needs nodevec to build allowed edges

	ExtractFullMatrix(g, "EdgeMatrix")
	ExtractVector(g, "EdgeMatrix", "NodeTmpFrom")

	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpFrom", "NodeTmpTwo")
	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpTwo", "NodeTmpThree")
	OperateMatrixOnVector(g, "EdgeMatrix", "NodeTmpThree", "NodeTmpFinal")

	S.PrintNodes(g, "NodeTmpFinal")

	fmt.Println("Normalize the result...")
}

// ***************************************************************

func ExtractFullMatrix(g S.Analytics, matrix string) {

	var err error

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

	var STtypes []string = []string{ "Follows", "Contains", "Expresses", "Near" }

	for coll := range STtypes {

		collection := STtypes[coll]

		copylinks := "FOR lnk IN " + collection + " INSERT {  _from: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._from).key), _to: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._to).key), weight: lnk.weight } INTO " + matrix

		_, err = g.S_db.Query(nil,copylinks,nil)
		
		if err != nil {
			fmt.Printf("copylinks failed: %v", err)
		}
		
		// Now symmetrize the matrix
			
		copylinks_symm := "FOR lnk IN " + collection + " INSERT {  _from: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._to).key), _to: CONCAT(\""+prefix+"\",PARSE_IDENTIFIER(lnk._from).key), weight: lnk.weight } INTO " + matrix
			
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

