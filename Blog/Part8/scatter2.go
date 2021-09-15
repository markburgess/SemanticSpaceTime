
package main

import (
	"fmt"
	"os"
	"strings"
	S "SST"
	A "github.com/arangodb/go-driver"
)

// ********************************************************************************
// * Run AFTER doors.go
// * Find all the nodes with the same in and out links. This shows symmetry
// * but doesn't result in aggregate coarse grains if there are multiple external lines
// ********************************************************************************

type SMatrix struct {
	
	In string   `json:"In"`
	Out string  `json:"Out"`
	Node string `json:"Agg"`
}

// ********************************************************************************

func main() {

	var dbname string = "TestSST"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics
	var slits S.Set = make(S.Set)

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	grains := FindCoarseGrains2(g,slits)

	for grain := range grains {

		fmt.Println("\n",grain,"\n")

		for slits := range grains[grain] {

			if !strings.HasPrefix(grains[grain][slits],"<") {
				fmt.Printf("     <-- %s\n", grains[grain][slits])
			}
		}
	}
}

//****************************************************

func FindCoarseGrains2(g S.Analytics,grains S.Set) S.Set {

	var doc SMatrix

	querystring := "FOR doc1 IN Follows FOR doc2 IN Follows FILTER doc1 != doc2 && doc1._to == doc2._from RETURN { \"In\": doc1._from, \"Out\": doc2._to, Agg: doc2._from}"

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Println("Query failed: %v", err)
		os.Exit(1)
	}

	defer cursor.Close()

	for {
		_,err = cursor.ReadDocument(nil,&doc)
		
		if A.IsNoMoreDocuments(err) {

			break

		} else if err != nil {

			fmt.Println("Doc returned: %v", err)
			os.Exit(1)

		} else {

			in := strings.Split(doc.In,"/")
			out := strings.Split(doc.Out,"/")
			key := "<" + in[1] + "|" + out[1] + ">"

			S.TogetherWith(grains,key,doc.Node)

		}
	}	

	return grains
}