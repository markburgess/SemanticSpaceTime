
package main

import (
	"fmt"
	"bufio"
	"strings"
	"os"
	S "SST"
	A "github.com/arangodb/go-driver"

)

// ****************************************************************************
// * Analyze Amazon sales data as a graph
// *
// * WARNING! This is a big data application, use MAXLINEs to limit time/cpu
// ****************************************************************************

// Data set from: http://snap.stanford.edu/ogb/data/nodeproppred/products.zip

const PATH = "/home/mark/LapTop/Work/Arango/DataSets/products/"
const TAXONOMY = "mapping/labelidx2productcategory.csv"
const HUBS = "raw/node-label.csv"
const FEATVEC = "raw/node-feat.csv"
const EDGES = "raw/edge.csv"

// As long as we don't use all the lines, some nodes will appear orphaned

const MAXLINES = 5000
const SIMILARITY_THRESHOLD = 25
const RENORM_THRESHOLD = 20

// ****************************************************************************

func main() {

	var dbname string = "AmazonSales"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	fmt.Println("Cross-link the product category hubs based on member linkage...first attempt")
 
	groups := CrossLinkHubs(g)

	ShowGroups(g,groups)
	ShowClusterBonds(g)

	fmt.Println("This basically leads to one giant cluster with weighted bonds, so we " + 
		"can renormalize by threshold to reveal dominant features") }

// ****************************************************************************

func CrossLinkHubs(g S.Analytics) S.Set {

	// Go by CONTAINS link collection to avoid multiple searches

	// For each product category, get members:
	// (a) if co-purchased with different category, link the *hubs* COACTIVE, increasing weight for each purchase
	// (b) could then try to add in the description vectors, but probably no effect

	// Look at the co-activation association and link the hubs it points to
	// The contains links point downward frmo hub to node, so we associate the "to" keys to pairs of hubs

	// We could possibly optimize this to make the COACTIVE link between hubs directly in AQL

	querystring := "FOR coactive in Near FILTER coactive.semantics == \"COACTIV\" FOR hub1 in Contains FILTER hub1._to == coactive._from && hub1._from LIKE \"Hubs/\\%\" FOR hub2 in Contains FILTER hub2._to == coactive._to && hub2._from LIKE \"Hubs/\\%\" RETURN {_from: hub1._from, _to: hub2._from}"

	// Make a matrix of links for the meta graph, counting strength as link weight

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Crosslink query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	var sets = make(S.Set)

	for {
		var doc S.Link
		meta,err := cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Link read in CrossLink \"%s\"failed: %v\n", meta,err)
		} else {
			if doc.From != doc.To {

				//fmt.Println("Linking hubs", doc.From, doc.To)

				S.TogetherWith(sets,doc.From,doc.To)

				f := strings.Split(doc.From,"/")
				t := strings.Split(doc.To,"/")
				
				from := NodeRef(f[0]+"/",f[1])
				to := NodeRef(t[0]+"/",t[1])
				
				S.IncrementLink(g, from, "COACTIV", to)
			}
		}
	}

	return sets
}

// ****************************************************************************

func ShowGroups(g S.Analytics, groups S.Set) {

	for gr := range groups {

		fmt.Println("Cluster",groups[gr])

		for hub := range groups[gr] {
			fmt.Println("  ",hub,":",groups[gr][hub],"=",S.GetNode(g,groups[gr][hub]))
		}
	}
}

// ****************************************************************************

func ShowClusterBonds(g S.Analytics) {

	querystring := "FOR doc IN Near FILTER doc.semantics == \"COACTIV\" && doc._from LIKE \"Hubs\\%\" RETURN doc"
	
	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Nodes query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	for {
		var doc S.Link
		meta,err := cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Node \"%s\"failed: %v\n", meta,err)
		} else {
			if doc.Weight > RENORM_THRESHOLD {
				fmt.Println("Relative bond:", doc.From, doc.To, doc.Weight)
			}
		}
	}
}

// ****************************************************************************
// Tools
// ****************************************************************************

func NodeRef(prefix,key string) S.Node {

	var node S.Node

	node.Key = key
	node.Prefix = prefix

	return node
}

// ****************************************************************************

func ProcessFileByLines(g S.Analytics,filename string,process_function func(S.Analytics,int,string)) {

	file, err := os.Open(filename)

	//fmt.Println("opening",PATH+EDGES)

	if err != nil {
		fmt.Printf("error opening file: %v\n",err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var line string
	var count int = 0 // indices start at 0 in the files
 
	for scanner.Scan() {
		line = scanner.Text()

		process_function(g,count,line)
		//fmt.Println(count,line,"\n")

		count++

		if count % 10000 == 0 {
			fmt.Println(count,"...")
		}

		if count > MAXLINES {
			break
		}
	}
 
	file.Close()
}