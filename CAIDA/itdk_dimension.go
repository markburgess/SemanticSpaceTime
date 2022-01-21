
package main

import (
	"fmt"
	"math"
	"os"
	"context"
	"time"
	"math/rand"
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

	var already = make(map[string]bool,0)
	var N_of_hop = make(map[int]float64,0)
	var min = make(map[int]float64,0)
	var max = make(map[int]float64,0)

	// For random starting nodes, explore to hop count = max 
	// and count the number of nodes within the distance

	// Node of ASes

	GetVolumeDistribution(g,already,N_of_hop,min,max)

	// Log-log plot to see if there is a power law line
	
	for r := 1; r <= len(N_of_hop); r++ {
		
		nlog := math.Log(N_of_hop[r])
		min := math.Log(min[r])
		max := math.Log(max[r])

		fmt.Printf("%d %f %f %f\n", r,nlog,min,max)
	}
}

// ********************************************************************************

func GetVolumeDistribution(g C.ITDK, already map[string]bool, N_of_hop map[int]float64, min map[int]float64, max map[int]float64) {

	var err error
	var cursor A.Cursor

	/* Getting arango to count nodes connected by ADJ/Near, if we start from IPv4 we 
           will get a different version

           FOR node IN 1..2 
             ANY 'Devices/N110547' Near OPTIONS { order: 'bfs',  uniqueVertices: 'global' } 
           COLLECT WITH COUNT INTO counter 

			if err != nil {           RETURN counter
        */

	// Choose a maximum length < diameter of graph to truncate

	var max_radius int = 20       // Seems to stabilize around here, and time grows exp -> 10 mins
	var max_samples float64 = 100
	var sample int = 0
	var starting int = 2000

	// First select a random sample set for statistics using the hash table

	all_nodes := make(map[string]bool)

	GetAllNodes(g, all_nodes,"Devices")
	//GetAllNodes(g, all_nodes,"IPv4")

	// Foreach starting node, count the volume of nodes within hops

	for node := range all_nodes {

		if sample > starting + int(max_samples) {

			break

		}

		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)

		if r1.Intn(10) < 5 {
			continue
		}

		sample++

		if sample < starting {
			continue
		}

		//fmt.Println(sample, "Start from ",node)

		for radius := 1; radius < max_radius; radius += 1 {

			qstring := fmt.Sprintf("FOR node IN 1..%d ANY 'Devices/%s' Near OPTIONS { order: 'bfs',  uniqueVertices: 'global' } COLLECT WITH COUNT INTO counter RETURN counter", radius,node)

			// This will take a long time, so we need to extend the timeout
			
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Hour*8))
			
			defer cancel()
			
			cursor,err = g.S_db.Query(ctx,qstring,nil)
			
			if err != nil {
				fmt.Printf("Query failed: %v", err)
				os.Exit(1)
			}
			
			defer cursor.Close()
			
			for {
				var count float64
				
				_,err = cursor.ReadDocument(nil,&count)
				
				if A.IsNoMoreDocuments(err) {
					break
				} else if err != nil {
					fmt.Printf("Doc returned: %v", err)
				} else {
					N_of_hop[radius] += count / max_samples

					if count < min[radius] {
						min[radius] = count
					}

					if count > max[radius] {
						max[radius] = count
					}
				}
			}
		}
	}
}

// ********************************************************************************

func GetAllNodes(g C.ITDK, all_nodes map[string]bool, collection string) {
	
	qstring := "FOR node IN "+collection+" RETURN node._key"			
	
	// This will take a long time, so we need to extend the timeout
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Hour*8))
	
	defer cancel()
	
	cursor,err := g.S_db.Query(ctx,qstring,nil)
	
	if err != nil {
		fmt.Printf("Query failed: %v", err)
		os.Exit(1)
	}
	
	defer cursor.Close()
			
	for {
		var key string
		
		_,err = cursor.ReadDocument(nil,&key)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			all_nodes[key] = true
		}
	}
}