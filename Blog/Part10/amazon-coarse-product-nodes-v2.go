
package main

import (
	"fmt"
	"strings"
	"strconv"
	"os"
	S "SST"
	A "github.com/arangodb/go-driver"

)

// ****************************************************************************
// Some parameters to control the coarse graining
// ****************************************************************************

// The larger we make it this square similarity radius, the coarser nodes will be

const SIMILARITY_THRESHOLD = 20

// ****************************************************************************

func main() {

	var dbname string = "AmazonSales"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	fmt.Println("Coarse grain similar nodes to reduce dimension, without category prejudice")
 
	clusters := ClusterVectorToFragments(g)
	ShowHubFractionsForEachCluster(g,clusters)

}

// ****************************************************************************

func ClusterVectorToFragments(g S.Analytics) S.Set {

	// First go through all nodes and get their Data vectors (if any)
	// Then decide which are close together in vector space 
	// (depending on representation assumptions)

	// Because this is computationally intense, look for shortcuts compared 
	// to first version

	var feature_vec map[string][]float64 = make(map[string][]float64)
	var querystring string

	fmt.Println("Trying to aggregate nodes without prejudice:")

	querystring = "FOR member IN Nodes RETURN member"

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

					feature_vec[doc.Key][i], err = strconv.ParseFloat(check_vec[i], 16)

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

		if i % 10 == 0 {
			fmt.Println("  ..",i)
		}
		
	}

	fmt.Println("  Reduced",len(keys),"to",len(clusters),"supernodes")
	return clusters
}

// ****************************************************************************

func ShowHubFractionsForEachCluster(g S.Analytics, clusters S.Set) {

	var nonzero int = 0

	for frag := range clusters {

		var composition [50]int

		for node := range clusters[frag] {

			h := GetCategoryFor(g,clusters[frag][node])
			composition[h]++
		}

		for h := range composition {
			if composition[h] != 0 {
				nonzero++
			}
		}
	}

	fmt.Println("Average hub composition of unbiased fragments =",float64(nonzero)/float64(len(clusters)))
}

// ****************************************************************************

func GetCategoryFor(g S.Analytics, key string) int {

	querystring := "FOR m IN Contains FILTER m._from LIKE \"Hubs/\\%\" && m._to == \"Nodes/"+key+"\" RETURN m._from"

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Category query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	for {
		var hub string

		meta,err := cursor.ReadDocument(nil,&hub)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Hub lookup \"%s\"failed: %v\n", meta,err)
		} else {
			var h int
			fmt.Sscanf(hub,"h_%d",&h)
			return h
		}
	}

return -1
}

// ****************************************************************************
// Tools
// ****************************************************************************

func Distance2(v1 []float64, v2 []float64) float64 {

	// Simplify this for speed compared to previous - non-Pythagorean

	var d2 float64 = 0

	for i := 0; i < len(v1); i++ {
		d2 += (v1[i] - v2[i])*(v1[i] - v2[i])
	}

	return d2
}

