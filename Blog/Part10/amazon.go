
package main

import (
	"fmt"
	"bufio"
	"strings"
	"strconv"
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

// ****************************************************************************

func main() {

	var dbname string = "AmazonSales"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	// Read files by line and do something with each line

	fmt.Println("Reading nodes")
	ProcessFileByLines(g,PATH+FEATVEC,AddNodeDescription)
	fmt.Println("Reading graph edges ... a lot to do!")
	ProcessFileByLines(g,PATH+EDGES,MakeCoPurchaseGraph)
	fmt.Println("Reading product descriptions")
	ProcessFileByLines(g,PATH+TAXONOMY,MakeTypeHubs)
	fmt.Println("Connecting products to hubs...a lot to do!")
	ProcessFileByLines(g,PATH+HUBS,ConnectTypeHubs)

	fmt.Println("Constructed graph, now compressing ... time consuming...")
 
	// Time consuming with MAXLINES > 50
	// AnnotateVectorNearness(g)

	// Finally, should we add links for hubs that overlap with other hubs,
	// i.e. cross-category shopping - also time consuming!

	groups := AnnotateHubNearness(g)

	ShowGroups(g,groups)
	ShowClusterBonds(g)
}

// ****************************************************************************

func MakeCoPurchaseGraph(g S.Analytics, n int, line string) {

	var fromto []string

	fromto = strings.Split(line,",")

	name_from := "n_" + fromto[0]
	name_to   := "n_" + fromto[1]

	// Add placeholders so we can link, to be supplemented with description later

	from := S.CreateNode(g, name_from, "", 0)
	to := S.CreateNode(g, name_to, "", 0)

	var coactivation_count float64 = 3 // this info is not recorded

	S.CreateLink(g, from, "COACTIV", to, coactivation_count)
}

// ****************************************************************************

func AddNodeDescription(g S.Analytics, n int, line string) {

	// load the word vectors - memory sensitive approach
	// only split the vector when we need to, add to Node.Data field

	//var vector []string
	//vector = strings.Split(line,",")
	//fmt.Println("vector dimension",len(vector))

	// Node the data seem incomplete .. not all nodes have these vectors

	var sales_per_month float64 = 23 // This info is not recorded

	node_key := fmt.Sprintf("n_%d",n)
	S.CreateNode(g, node_key, line, sales_per_month)

}

// ****************************************************************************

func MakeTypeHubs(g S.Analytics, n int, line string) {

	if n > 0 { // skip first line header

		nodedescr := strings.Split(line,",")

		name := "h_" + nodedescr[0]
		descr := nodedescr[1]
		
		var strategic_weight float64 = 99 // sales per month data is missing

		S.CreateHub(g, name, descr, strategic_weight)
	}
}

// ****************************************************************************

func ConnectTypeHubs(g S.Analytics, n int, line string) {

	node_key := fmt.Sprintf("n_%d",n)
	hub_key := fmt.Sprintf("h_%s",line)

	node := NodeRef("Nodes/",node_key)
	hub := NodeRef("Hubs/",hub_key)

	// Links to product type hubs have no weight in the current top down scheme
	// Items are only named taxonomically once

	S.CreateLink(g, hub, "CONTAINS", node, 1)

}

// ****************************************************************************

func AnnotateVectorNearness(g S.Analytics) {

	// First go through all node and get their Data vectors (if any)
	// Then decide which are close together in vector space 
	// (depending on representation assumptions)

	var feature_vec map[string][]float64 = make(map[string][]float64)
	var querystring string
	
	querystring = "FOR doc IN Nodes RETURN doc"
	
	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Nodes query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	for {
		var doc S.Node
		meta,err := cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Node \"%s\"failed: %v\n", meta,err)
		} else {
			//fmt.Println("Node", doc.Key, doc.Data)

			check_vec := strings.Split(doc.Data,",")

			_, err := strconv.ParseFloat(check_vec[0], 4)

			if err == nil {

				feature_vec[doc.Key] = make([]float64,100)

				for i := 0; i < len(check_vec); i++ {

					// This might be too big for RAM with large graphs

					feature_vec[doc.Key][i], err = strconv.ParseFloat(check_vec[i], 64)

					if err != nil{
						fmt.Println("Can't parse expected float number",check_vec[i])
						os.Exit(1)
					}
				}
			}
		}
	}
	
	// Now we want the distances between all pairs, so get a vector by number rather than by name
	
	var keys []string
	
	for n1 := range feature_vec {
		keys = append(keys,n1)
	}
	
	// Create a NEAR link weighted by vector distance
	
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			ni := NodeRef("Nodes/",keys[i])
			nj := NodeRef("Nodes/",keys[j])
			d2 := Distance2(feature_vec[keys[i]],feature_vec[keys[j]])

			if d2 < SIMILARITY_THRESHOLD {
				//fmt.Println("Similar products?",ni.Key,nj.Key,d2)
				S.CreateLink(g, ni, "IS_LIKE", nj, d2)
			}
		}
	}
}

// ****************************************************************************

func AnnotateHubNearness(g S.Analytics) S.Set {

	// Go by hub CONTAINS collection to avoid multiple searches

	// For each product category, get members:
	// (a) if co-purchased with different category, link the *hubs* COACTIVE, increasing weight for each purchase
	// (b) could then try to add in the description vectors, but probably no effect

	// Look at the co-activation association and link the hubs it points to
	// The contains links point downward frmo hub to node, so we associate the "to" keys to pairs of hubs

	// We could possibly optimize this to make the COACTIVE link between hubs directly in AQL

	querystring := "FOR coactive in Near FILTER coactive.semantics == \"COACTIV\" FOR hub1 in Contains FILTER hub1._to == coactive._from FOR hub2 in Contains FILTER hub2._to == coactive._to RETURN {_from: hub1._from, _to: hub2._from}"

	// Make a matrix of links for the meta graph, counting strength as link weight

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Nodes query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	var sets = make(S.Set)

	for {
		var doc S.Link
		meta,err := cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Node \"%s\"failed: %v\n", meta,err)
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
			fmt.Println("Relative bond", doc.From, doc.To, doc.Weight)
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

func Distance2(v1 []float64, v2 []float64) float64 {

	var d2 float64 = 0

	for i := 0; i < len(v1); i++ {
		d2 += (v1[i] - v2[i]) * (v1[i] - v2[i])
	}

	//fmt.Println("distance ",d2)
	return d2
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