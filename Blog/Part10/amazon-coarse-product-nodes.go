
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

// ****************************************************************************
// Some parameters to control the coarse graining
// ****************************************************************************

// As long as we don't use all the input lines, some nodes will appear orphaned

const MAXLINES = 50000

// The larger we make it this square similarity radius, the coarser nodes will be

const SIMILARITY_THRESHOLD = 20 

// ****************************************************************************

func main() {

	var dbname string = "AmazonSales"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	fmt.Println("Within each category hub, coarse grain similar nodes to reduce dimension")
 
	// This could be optimized further, but let's show the logic

	keys,names := GetProductCategories(g)

	for k := range keys {

		cluster := ClusterVectorToFragments(g,k,keys[k],names[k])

		CoarseGrainNodesToFragments(g,cluster,keys[k],names[k])
	}

	fmt.Println("\nNow crosslink the fragments by coactivity")

	groups := CrossLinkFragments(g)

	ShowGroups(g,groups)
	ShowClusterBonds(g)

}

// ****************************************************************************

func GetProductCategories(g S.Analytics) ([]string,[]string) {

	var names,keys []string

	querystring := "FOR member IN Hubs RETURN member"

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Category query \"%s\"failed: %v", querystring,err)
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
			// Some names are quoted and some are empty!

			var name string

			if doc.Data == "" {
				name = "(unnamed)"
			} else {

				name = strings.Trim(doc.Data,"\"")
			}

			names = append(names,name)
			keys = append(keys,doc.Key)
		}
	}

	return keys,names
}

// ****************************************************************************

func ClusterVectorToFragments(g S.Analytics, cat int, key, name string) S.Set {

	// First go through all nodes in a category and get their Data vectors (if any)
	// Then decide which are close together in vector space 
	// (depending on representation assumptions)

	var feature_vec map[string][]float64 = make(map[string][]float64)
	var querystring string

	fmt.Println("Trying to aggregate category:",name)

	querystring = "FOR member IN Contains FILTER member._from == \"Hubs/"+key+"\" RETURN DOCUMENT(member._to)"

	// NB, there are zero vectors (0,0,0,0,0...)
	// If we don't read all the data nodes, then some edges may point to nodes that don't exist

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Category query \"%s\"failed: %v", querystring,err)
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
			// The data are not yet in numeric format, they are strings

			check_vec := strings.Split(doc.Data,",")

			_, err := strconv.ParseFloat(check_vec[0], 4)

			if err == nil {

				feature_vec[doc.Key] = make([]float64,100)

				for i := 0; i < len(check_vec); i++ {

					// This might be too big for RAM with large graphs!

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
	
	// Cluster together things within a similarity radius
	// Firt Come First Served, if equi-distant between subclusters

	var clusters S.Set = make(S.Set)
	
	for i := 0; i < len(keys); i++ {

		for j := i + 1; j < len(keys); j++ {

			d2 := Distance2(feature_vec[keys[i]],feature_vec[keys[j]])

			if d2 < SIMILARITY_THRESHOLD {

				S.TogetherWith(clusters,keys[i],keys[j])
			}
		}
	}

	fmt.Println("  Reduced",len(keys),"to",len(clusters),"supernodes")
	return clusters
}

// ****************************************************************************

func CoarseGrainNodesToFragments(g S. Analytics, cluster S.Set, hubkey, category string) {

	// Now link every member to a new "fragment of hub" node in Frags collection

	for sub := range cluster {

		frag_hub_name := sub + "_of_" + hubkey

		//fmt.Println("  Create hub",frag_hub_name)

		frag := S.CreateFragment(g,frag_hub_name,"sub part of " + category)

		for member := range cluster[sub] {

			// Weight the link by the size of the cluster
			// fmt.Println("  Fragment",frag_hub_name,"contains",cluster[sub][member])

			node := NodeRef("Nodes/",cluster[sub][member])

			S.CreateLink(g,frag,"GENERALIZES",node,float64(len(cluster[sub])))
		}
	}

}

// ****************************************************************************

func CrossLinkFragments(g S.Analytics) S.Set {

	// This is the analogue of CrossLinkHubs(g S.Analytics) in category-level-graph
	// Look throught the raw node coactivations and link their Fragments they belong to
	// count the strength by incrementing the weight

	// For each copurchase link between nodes, look at the fragments they belong to, then extract the 
	// fragments and link them in the approximation plane. Since several may match, increment the link
	// weight for each.

	querystring := "FOR coactive in Near FILTER coactive.semantics == \"COACTIV\" FOR frag1 in Contains FILTER frag1._to == coactive._from && frag1._from LIKE \"Fragments/\\%\" FOR frag2 in Contains FILTER frag2._to == coactive._to && frag2._from LIKE \"Fragments/\\%\" RETURN {_from: frag1._from, _to: frag2._from}"

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

	querystring := "FOR doc IN Near FILTER doc.semantics == \"COACTIV\" && doc._from LIKE \"Fragments/\\%\" RETURN doc"
	
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