
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

	fmt.Println("Data loaded, maxlines (approx node count)",MAXLINES)
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