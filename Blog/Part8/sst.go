

package main

import (
	"fmt"
	S "SST"
)

// ********************************************************************************

func main() {
	fmt.Println("Building a SST graph")

	// "http://localhost:8529", "https://5a8db333269f.arangodb.cloud:8529/"
        // root, mark ; "root", "wnbGnPuaAy4dEpCXHwbP"

	var dbname string = "TestSST"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	S.InitializeSmartSpaceTime()

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	nodes := []S.Node{

		S.Node{
			"melville_moby_dick",
			"Moby Dick",
			"novel",
			2,
		},

		S.Node{
			"dickens_two_cities",
			"Tale of Two Cities",
			"novel",
			4,
		},

		S.Node{
			"star_trek",
			"Wrath of Khan",
			"movie",
			3,
		},

		S.Node{
			"sentence_best_of_times",
			"It was the best of times, it was the worst of times.",
			"quote",
			12,
		},

		S.Node{
			"sentence_ahab",
			"Call me Ahab.",
			"quote",
			3,
		},
	}

	links := []S.Link{

		S.Link {
			From: "Nodes/melville_moby_dick", 
			To: "Nodes/sentence_ahab",
			SId: "CONTAINS",
			Weight: 1.2,
		},

		S.Link {
			From: "Nodes/dickens_two_cities", 
			To: "Nodes/sentence_best_of_times", 
			SId: "CONTAINS",
			Weight: 1.2,
		},

		S.Link {
			From: "Nodes/star_trek", 
			To: "Nodes/sentence_best_of_times", 
			SId: "CONTAINS",
			Weight: 6.2,
		},

	}

	// Convergent additions

	fmt.Println("========== New safe/convergent interface ==============")

	for node := range nodes {
		S.AddNode(g,nodes[node])
	}

	for link := range links {
		S.AddLink(g,links[link])
	}

	fmt.Println("========== SHOW NODES ==============")

	S.PrintNodes(nil,g.S_db)
	
	fmt.Println("========== show best of times ==============")

	var node string = "Nodes/sentence_best_of_times"
	fmt.Println("Neighbours (-) of id (contains)",node)
	list := S.GetNeighboursOf(g,node,S.GR_CONTAINS)

	fmt.Println("- : ",list)

	node = "Nodes/star_trek"
	fmt.Println("Neighbours (+) of id (contains)",node)
	list = S.GetNeighboursOf(g,node,S.GR_CONTAINS)

	fmt.Println("+ : ",list)

	// ********** Import invariants

	//var STtype = make(map[string]int)

	//S.LoadIntKV2Map(g.S_db,"ST_Types", STtype)


}
