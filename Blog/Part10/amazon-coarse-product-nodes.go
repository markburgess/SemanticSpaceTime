
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

	fmt.Println("Within each category hub, coarse grain similar nodes to reduce dimension")
 
	AnnotateVectorNearness(g)

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
	
	// Create a NEAR-type link weighted by vector distance
	
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