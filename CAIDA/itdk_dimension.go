
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

const max_hop_radius = 20  // Smaller radius as there is no long range order
const symm_factor = 1.5 // defines the spacefilling geometry nature (communication strength)

// ********************************************************************************

func main() {
	
	var dbname string = "ITDK-snapshot-model"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g C.ITDK
	
	g = C.OpenITDK(dbname, service_url, user, pwd)

	// Get node degree distributions from the Near relations

	var samples = make(map[int][]float64,0)	
	var max = make(map[string]float64,0)
	//var startnode string = ",50,50"

	// Do a random sample set or a calibration

	InitializeSamples(samples)

	collection := "Devices" // IPv4, Devices

	startnodes := ScatterSamples(g, collection,10)

	for s := range startnodes {
		GetVolumeDistribution(g, collection, startnodes[s], samples, max)
	}

	GetStats(samples, max)

}

// ********************************************************************************

func InitializeSamples(samples map[int][]float64) {

	for hop_radius := 1; hop_radius < max_hop_radius; hop_radius += 1 {

		samples[hop_radius] = make([]float64,0)
	}	
}
// ********************************************************************************

func GetVolumeDistribution(g C.ITDK, collection string, startnode string, samples map[int][]float64, max map[string]float64) {

	var err error
	var cursor A.Cursor
	var effvolume = make(map[int]float64,0)

	fmt.Println("Start from ",collection,startnode)
	
	for hop_radius := 1; hop_radius < max_hop_radius; hop_radius += 1 {
		
		qstring := fmt.Sprintf("FOR node IN 1..%d ANY '%s/%s' Near OPTIONS { order: 'bfs',  uniqueVertices: 'global' } COLLECT WITH COUNT INTO counter RETURN counter", hop_radius,collection,startnode)
		
		// This might take a long time, so we need to extend the timeout
		
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
				effvolume[hop_radius] = count
				//fmt.Println("hop_radius",hop_radius,"count",count, effvolume[hop_radius])
			}
		}
	}
	
	// Log-log plot to see if there is a power law line
	
	var grad float64
	
	for hops := 1; hops <= len(effvolume); hops++ {
		
		// calculate delta/delta for each step and for wholes...

		if hops > 2 {
			deltay := math.Log(effvolume[hops])            // log V
			deltax := math.Log(float64(hops)*symm_factor)  // log r
			grad = deltay/deltax
		}

		samples[hops] = append(samples[hops],grad)

		if max[startnode] < grad {
			max[startnode] = grad
		}

		//fmt.Printf("(%s) %d %f (grad = %f) %f\n",startnode,hops,effvolume[hops],grad,max[startnode])
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

// ********************************************************************************

func GetStats(samples map[int][]float64, max map[string]float64) {

	for hops := 1; hops < max_hop_radius; hops += 1 {

		var dim,vardim float64 = 0,0

		n := float64(len(samples[hops]))

		for s := range samples[hops] {

			dim += samples[hops][s]

		}

		dim = dim / n

		for s := range samples[hops] {

			vardim += (samples[hops][s]-dim) * (samples[hops][s]-dim)
		}

		vardim = vardim / n

		fmt.Printf("%d %f %f\n",hops, dim, math.Sqrt(vardim))
	}

	for mx := range max {
		fmt.Printf("Max (%s) %f\n",mx,max[mx])
	}
}

// ********************************************************************************

func ScatterSamples(g C.ITDK, collection string, max_samples int) []string {

	// First select a random sample set for statistics using the hash table

	all_nodes := make(map[string]bool)
	var list []string
	var sample int = 0

	GetAllNodes(g, all_nodes,collection)

	s0 := rand.NewSource(time.Now().UnixNano())
	r0 := rand.New(s0)
	offset := r0.Intn(len(all_nodes)/3)
	
	// Foreach offset node, count the volume of nodes within hops
	
	for node := range all_nodes {
		
		if sample >= offset + max_samples {
			
			break
			
		}
		
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		
		if r1.Intn(10) < 5 {
			continue
		}
		
		sample++
		
		if sample < offset {
			continue
		}
	
		list = append(list,node)
	}

	fmt.Println(list)
	return list
}