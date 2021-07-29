
package main

import (
	"fmt"
	S "SST"
)

// ********************************************************************************

func main() {

	var dbname string = "centrality1"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	nodes := []S.Node{

		S.Node{
			"node1",
			"Satellite 1",
			"nodes",
			1.0,
		},

		S.Node{
			"node2",
			"Stellite 2",
			"nodes",
			1.0,
		},

		S.Node{
			"node3",
			"Satellite 3",
			"nodes",
			1.0,
		},

		S.Node{
			"node4",
			"Satellite 4",
			"nodes",
			1.0,
		},

		S.Node{
			"node5",
			"Satellite 5",
			"nodes",
			1.0,
		},
	}

	links := []S.Link{

		S.Link { 
			From: "Nodes/node1", 
			To: "Nodes/node2",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},

		S.Link { 
			From: "Nodes/node2", 
			To: "Nodes/node3",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node3", 
			To: "Nodes/node4",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node4", 
			To: "Nodes/node1",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node1", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node2", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node3", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},
		S.Link { 
			From: "Nodes/node4", 
			To: "Nodes/node5",
			SId: "CONNECTED",
			Weight: 1,           // Initialize all the same, positive
		},


	}

	// Convergent additions

	fmt.Println("========== NEW IDEMP FNs ==============")

	for node := range nodes {
		S.AddNode(g,nodes[node])
	}
	
	for link := range links {
		S.AddLink(g,links[link])
	}
	
	fmt.Println("========== SHOW NODES ==============")
	
	S.PrintNodes(g.S_db)

	fmt.Println("========== SHOW ADJ[node,node] ==============")

	adjacency_matrix_keys := S.GetAdjacencyMatrixByKey(g,"CONNECTED",false)

	fmt.Println(adjacency_matrix_keys)

	fmt.Println("========== SHOW ADJ[i,j] ==============")

	adjacency_matrix,dim,keys := S.GetAdjacencyMatrixByInt(g,"CONNECTED",false)

	S.PrintMatrix(adjacency_matrix,dim,keys)

	fmt.Println("========== SHOW principal ev[i] ==============")

	ev := S.GetPrincipalEigenvector(adjacency_matrix,dim)

	S.PrintVector(ev,dim,keys)
}

