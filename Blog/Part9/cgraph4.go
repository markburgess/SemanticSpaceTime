
package main

import (
	"fmt"
	S "SST"
)

// ********************************************************************************

func main() {

	var dbname string = "SemanticSpacetime"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	// Assume a graph from running scan.go

	fmt.Println("========== GET UNFILTERED, SYMMETRIZED ADJ[i,j] ==============")

	adjacency_matrix,dim,keys := S.GetFullAdjacencyMatrix(g,true)

	fmt.Println("Connected nodes (by collection):",dim)

	fmt.Println("========== SHOW principal ev[i] ==============")

	ev := S.GetPrincipalEigenvector(adjacency_matrix,dim)

	S.PrintVector(ev,dim,keys)
}

