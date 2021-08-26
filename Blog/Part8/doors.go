
package main

import (
	"fmt"
	"strings"
	S "SST"
)

// ********************************************************************************
// * Illustrate path analysis, future causality cone ++
// ********************************************************************************

func main() {

	var dbname string = "TestSST"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	nodes := []S.Node{

		S.Node{
			"start_L0",
			"The beginning of the journey",
			"nodes",
			1.0,
		},

		S.Node{
			"door1_L1",
			"Then there was a door to the next level",
			"nodes",
			1.0,
		},

		S.Node{
			"door2_L1",
			"A door appeared out of nowhere",
			"nodes",
			1.0,
		},

		S.Node{
			"hole_L1",
			"There was a small hole in the fence.",
			"nodes",
			1.0,
		},

		S.Node{
			"gate_L1",
			"There was a gate in the middle of the fence.",
			"nodes",
			1.0,
		},

		S.Node{
			"passage_L2",
			"A passage ran along the river.",
			"nodes",
			1.0,
		},

		S.Node{
			"road_L2",
			"The small road ran parallel to the hedge.",
			"nodes",
			1.0,
		},

		S.Node{
			"river_L2",
			"The river itself could carry a person in its current.",
			"nodes",
			1.0,
		},

		S.Node{
			"tram_L2",
			"A very slow tram takes you along its rusty tracks.",
			"nodes",
			1.0,
		},

		S.Node{
			"bike_L2",
			"A man carries you on his bike.",
			"nodes",
			1.0,
		},

		S.Node{
			"target1_L3",
			"The ocean at the end of the lane.",
			"nodes",
			1.0,
		},

		S.Node{
			"target2_L3",
			"The little shop of horrors.",
			"nodes",
			1.0,
		},

		S.Node{
			"target3_L3",
			"Bulls-eye on the interferometer!",
			"nodes",
			1.0,
		},

	}

	links := []S.Link{

		// L1

		S.Link { 
			From: "Nodes/start_L0", 
			To: "Nodes/door1_L1",
			SId: "NEXT",
			Weight: 1,
		},

		S.Link {
			From: "Nodes/start_L0", 
			To: "Nodes/door2_L1", 
			SId: "NEXT",
			Weight: 2,
		},

		S.Link {
			From: "Nodes/start_L0", 
			To: "Nodes/hole_L1", 
			SId: "NEXT",
			Weight: 3,
		},

		S.Link {
			From: "Nodes/start_L0", 
			To: "Nodes/gate_L1", 
			SId: "NEXT",
			Weight: 4,
		},


		// L2

		S.Link {
			From: "Nodes/door1_L1", 
			To: "Nodes/passage_L2", 
			SId: "NEXT",
			Weight: 1.2,
		},

		S.Link {
			From: "Nodes/door1_L1", 
			To: "Nodes/passage_L2", 
			SId: "LEADS_TO",
			Weight: 1.8,
		},

		S.Link {
			From: "Nodes/door1_L1", 
			To: "Nodes/road_L2", 
			SId: "NEXT",
			Weight: 2.2,
		},

		S.Link {
			From: "Nodes/door1_L1", 
			To: "Nodes/river_L2", 
			SId: "NEXT",
			Weight: 3.2,
		},

		S.Link {
			From: "Nodes/door2_L1", 
			To: "Nodes/river_L2", 
			SId: "NEXT",
			Weight: 4.2,
		},

		S.Link {
			From: "Nodes/door2_L1", 
			To: "Nodes/tram_L2", 
			SId: "NEXT",
			Weight: 5.2,
		},

		S.Link {
			From: "Nodes/hole_L1", 
			To: "Nodes/tram_L2", 
			SId: "NEXT",
			Weight: 6.2,
		},

		//

		S.Link {
			From: "Nodes/gate_L1", 
			To: "Nodes/tram_L2", 
			SId: "NEXT",
			Weight: 7.2,
		},

		S.Link {
			From: "Nodes/gate_L1", 
			To: "Nodes/bike_L2", 
			SId: "NEXT",
			Weight: 8.2,
		},

		// L3

		S.Link {
			From: "Nodes/passage_L2", 
			To: "Nodes/target1_L3", 
			SId: "NEXT",
			Weight: 6.2,
		},

		S.Link {
			From: "Nodes/road_L2", 
			To: "Nodes/target2_L3", 
			SId: "NEXT",
			Weight: 6.2,
		},

		S.Link {
			From: "Nodes/river_L2", 
			To: "Nodes/target3_L3", 
			SId: "NEXT",
			Weight: 6.2,
		},

		S.Link {
			From: "Nodes/tram_L2", 
			To: "Nodes/target3_L3", 
			SId: "NEXT",
			Weight: 6.2,
		},

		S.Link {
			From: "Nodes/bike_L2", 
			To: "Nodes/target3_L3", 
			SId: "NEXT",
			Weight: 6.2,
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
	
	S.PrintNodes(g, "Nodes")
	
	var visited = make(map[string]bool)
	
	var cone = make(S.Cone)
	var pathdim int

	fmt.Println("========== SHOW SPACELIKE CONE LAYERS ==============")
	
	cone,pathdim = S.GetPossibilityCone(g, "Nodes/start_L0", -S.GR_FOLLOWS,visited)	
	
	for layer := 0; layer < len(cone); layer++ {

		fmt.Println("Timestep (layer)",layer,"paths",pathdim)

		for n := range cone[layer] {

			var mixed_links string = "( "

			for linktypes := range cone[layer][n] {

				if len(mixed_links) > 2 {
					mixed_links = mixed_links + " or "
				}

				mixed_links = mixed_links + cone[layer][n][linktypes].LinkType
			}

			mixed_links = mixed_links + " )"
			
			fmt.Println("    ",layer, ":", mixed_links, n)
			
		}
	}

	fmt.Println("========== SHOW TIMELIKE CONE PATHS ==============")

	paths := S.GetConePaths(g, "Nodes/start_L0", -S.GR_FOLLOWS,visited)

	for path := 0; path < len(paths); path++ {

		fmt.Println(path,paths[path],"\n")
	}

	fmt.Println("========== SHOW TIMELIKE SUPERNODE / S-MATRIX PATHS ==============")

	agg,groups := FindSymmetricBetweenNodes(paths)

	fmt.Println("\nSymmetrized super-node paths:\n")

	for path := 0; path < len(agg); path++ {

		fmt.Println(path,agg[path],"\n")
	}
	
	fmt.Println("\nSymmetrized nodes:\n")

	var gr int = 1

	for group := range groups {

		for member := range groups[group] {
			fmt.Printf("symm. supernode_%d == %s\n",gr,groups[group][member])
		}

		gr++
		fmt.Println()
	}
}

//****************************************************

func FindSymmetricBetweenNodes(paths []string) ([]string,S.Set) {

	var n1,n2 [200][]string
	var sets = make(S.Set)

	for path := 0; path < len(paths); path++ {
		
		n1[path] = strings.Split(paths[path], ":")
		
		for match := path+1; match < len(paths); match++ {

			n2[match] = strings.Split(paths[match], ":")

			var maxdepth int

			if len(n2) < len(n1) {
				maxdepth = len(n2)
			} else {
				maxdepth = len(n1)
			}

			maxdepth = 6

			for depth := 0; depth < maxdepth-2; depth = depth + 2 {
			
				if (n1[path][depth] == n2[match][depth]) && (n1[path][depth+4] == n2[match][depth+4]) {
					fmt.Println("Join nodes",path,n1[path][depth+2],match,n2[match][depth+2],"because between",n1[path][depth],"and",n1[path][depth+4])

					S.TogetherWith(sets,n1[path][depth+2],n2[match][depth+2])
				}
			}
		}
	}

	// reformat

	var newpaths []string
		
	for path := 0; path < len(paths); path++ {
		
		var newpath string

		for depth := 0; depth < len(n1[path]); depth++ {
	
			if depth/2 == 0 {
				// link	
				newpath = newpath + n1[path][depth] + " :\n"
			} else {
				// node
				exists,newname,members := S.BelongsToSet(sets,n1[path][depth])

				if exists {
					newpath = newpath + newname + "=(" + members + ")" + " :\n"
				} else {
					newpath = newpath + n1[path][depth] + " :\n"
				}
			}
		}

		var repeat bool = false

		for p := range newpaths {

			if newpaths[p] == newpath {
				repeat = true
				break
			}
		}

		if !repeat {
			newpaths = append(newpaths,newpath)
		}
	}

	return newpaths, sets
}
