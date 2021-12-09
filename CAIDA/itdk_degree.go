
package main

import (
	"fmt"
	"sort"
	"math"
	C "CAIDA_SST"
	A "github.com/arangodb/go-driver"

)

// ********************************************************************************
// Find the degree distribution of the nodes in the graph
// ********************************************************************************

type Adjacency struct {

	K  string `json:"K"`
	V  int    `json:"V"`
}

// ********************************************************************************

func main() {
	
	var dbname string = "ITDK-snapshot-model"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g C.ITDK
	
	g = C.OpenITDK(dbname, service_url, user, pwd)

	// Get node degree distributions from the Near relations

	var k_in = make(map[string]int,0)
	var k_out = make(map[string]int,0)
	var Nk = make(map[int]int,0)

	GetDegreeDistribution(g,k_in,k_out,Nk)

	var N []int
	var key = make(map[int]int)

	// Copy the distrbution into a sortable array

	for k := range Nk {
		if Nk[k] > 0 {			
			N = append(N,Nk[k])
			key[Nk[k]] = k
		}
	}

	// Sort the numbers

	sort.Ints(N)

	// Log-log plot to see if there is a power law line
	
	for k := len(N)-1; k >= 0; k-- {
		
		nlog := math.Log(float64(N[k]))
		klog := math.Log(float64(key[N[k]]))

		if nlog > 0 {
			fmt.Printf("%10f %f\n", klog,nlog)
		}
	}
}

// ********************************************************************************

func GetDegreeDistribution(g C.ITDK, k_in,k_out map[string]int, N map[int]int) {

	var err error
	var cursor A.Cursor

	// Here just looking at all the adjacency relations ADJ_* of type Near
	// could add a filter, e.g. FOR n in Near FILTER n.semantics == "ADJ_NODE"

	instring := "FOR n in Near COLLECT node = n._to INTO inn RETURN { K: node, V: COUNT(inn[*])}"
	outstring := "FOR n in Near COLLECT node = n._from INTO out RETURN { K: node, V: COUNT(out[*])}"

	cursor,err = g.S_db.Query(nil,instring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
		os.Exit(1)
	}

	defer cursor.Close()

	for {
		var doc Adjacency

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			k_in[doc.K] = doc.V
		}
	}

	cursor,err = g.S_db.Query(nil,outstring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
		os.Exit(1)
	}

	defer cursor.Close()

	for {
		var doc Adjacency

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			k_out[doc.K] = doc.V
			k := k_out[doc.K] + k_in[doc.K]
			N[k]++
		}
	}
}
