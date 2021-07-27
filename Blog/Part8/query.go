//
// Copyright © Mark Burgess, ChiTek-i (2020)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"flag"
	//"strings"
	"fmt"
	"os"
	//"sort"
	S "SST"
	A "github.com/arangodb/go-driver"

)

// ****************************************************************************
// basic query of graph structure
// ****************************************************************************

func main() {

	// 1. test cellibrium graph


	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	S.InitializeSmartSpaceTime()
		
	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)
		
	if len(args) < 1 {
		ListConcepts(g);
		os.Exit(0);
	}

	start := "Nodes/"+args[0]

	// Generate stories
	
	description := S.GetNode(g,start)
	fmt.Printf("\n(%s) \"%s\"\n",start,description)

	// Show Cones Retarded, Advanced and Generalized

	pairs := S.GetNeighboursOf(g,start,S.GR_CONTAINS,"+")
	
	for p := range pairs {
		for m := range pairs[p] {
			fmt.Println("(",pairs[p][m].From,") ",pairs[p][m].LinkType,S.GetNode(g,pairs[p][m].From))
		}
	}

}

//**************************************************************

func usage() {
    fmt.Fprintf(os.Stderr, "usage: go run query.go [filelist]\n")
    flag.PrintDefaults()
    os.Exit(2)
}

//**************************************************************

func ListConcepts(g S.Analytics) {

// description / name

	var err error
	var cursor A.Cursor
	var counter int = 1
	var doc S.Node

	querystring := "FOR doc IN Nodes RETURN doc" // or ._id

	cursor,err = g.S_db.Query(nil,querystring,nil)

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
	
			fmt.Printf("%4d : %s : %.100s ...\n",counter,doc.Key,doc.Data)
			
			counter++
		}
	}	
}

