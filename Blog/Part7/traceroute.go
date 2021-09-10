
package main

import (
	"flag"
	"os"
	"os/exec"
	"io/ioutil"
	"net"
	"fmt"
	"sort"
	"strings"
	S "SST"
	A "github.com/arangodb/go-driver"
)

type Hop struct {

	Key string
	Tuple []string
}

var NOWHERE Hop = Hop{ Key: "", Tuple: []string{} }

// ********************************************************************************
// * Traceroute model IPv4
// ********************************************************************************

func main() {

	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	fmt.Println(args)

	if len(args) != 1 {
		usage()
		os.Exit(0);
	}

	path := TraceRoute(args[0])
	ParseTrace(path)
}

// **********************************************************************

func usage() {
    fmt.Fprintf(os.Stderr, "usage: go run traceroute.go [destination]\n")
    flag.PrintDefaults()
    os.Exit(2)
}

// ***********************************************************************

func TraceRoute(destination string) []string {

	command := exec.Command("/usr/sbin/traceroute","-n",destination)

	stream, err := command.StdoutPipe()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	command.Start()
	bytes, _ := ioutil.ReadAll(stream)
	command.Wait()

	output := string(bytes)
	lines := strings.Split(output,"\n")

	return lines
}

// ***********************************************************************

func ParseTrace(trace []string) {

	var dbname string = "Internet"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	var g S.Analytics
	var hops []Hop = make([]Hop,1)
	var next Hop

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	// First line contains destination IP

	ex := strings.Split(trace[0],"(")
	ip := strings.Split(ex[1],")")

	destination := ip[0]

	// We want to do a two pass lookahead to gauge multislits

	for line := 1; line < len(trace) - 1; line++ {

		var hop Hop
		var empty bool = true
		chop := strings.Split(trace[line]," ")

		hop.Tuple = make([]string,0)

		for p := 0; p < len(chop); p++ {
			
			if chop[p] == "*" || net.ParseIP(chop[p]) != nil {
				
				hop.Tuple = append(hop.Tuple,chop[p])
				empty = false
			}
		}

		if empty {
			continue
		}

		// Make a unique key for the cluster

		sort.Strings(hop.Tuple)
		hop.Key = hop.Tuple[0]

		for addr := 1; addr < len(hop.Tuple); addr++ {
			
			hop.Key = hop.Key + "_" + hop.Tuple[addr]
		}
		
		hops = append(hops,hop)
	}

	// The final destination must be a single IP, but it could be blocked, so we can use the original

	var hop,mouth Hop
	var wormhole int = 0

	hop.Key = destination
	hop.Tuple = []string{destination}
	hops = append(hops,hop)

	// now work through the spacelike hypersections and skip longitudinal wormholes
	// if the next node appears as *_*_* then we've hit a dark ICMP region "wormhole"
	// mark this as the mouth of the wormhole

	for h := 1; h < len(hops); h++ {

		// Normal Internet space
		
		if h < len(hops) -1 {
			next = hops[h+1]
		} else {
			next = NOWHERE
		}
				
		if wormhole > 0 {
			
			if next.Key != "*_*_*" {
				
				// Exiting workhole, now we measured the tunnel, record it as one longitudinal supernode
				
				tunnel := Hop{ Key: fmt.Sprintf("wormhole_%d_from_%s_to_%s",wormhole,mouth.Key,next.Key), Tuple: next.Tuple }
				nodes,comments := CoActivationGroupMerge(&g,tunnel)
				S.NextParallelEvents(&g,nodes,comments)
				
				// Treat the entire wormhole as a single tunnel node
				wormhole = 0
				continue
				
			} else {
				// Inside the wormhole, count the hops
				wormhole++
			}
		}

		if wormhole == 0 {

			if hops[h].Key != "*_*_*" && next.Key == "*_*_*" {
				mouth = hops[h]
				wormhole++
			}
			
			gateways, comments := CoActivationGroupMerge(&g,hops[h])
			S.NextParallelEvents(&g,gateways,comments)
		}
		
	}
}

//****************************************************

func CoActivationGroupMerge(g *S.Analytics, this Hop) ([]string,[]string) {

	// Are there any existing nodes with exit points in the same entry/exit groups?
	// If we find any IP from this in such a group, merge this group with that group
	// with the same hub name

	fmt.Printf("Hop: %s -> %s\n",S.PreviousEvent(g).Key,this.Key)

	var comments []string = make([]string,0)

	if len(this.Tuple) == 1 {
		comments = append(comments,"singleton node")
		return this.Tuple, comments
	}

	for fan := 0; fan < len(this.Tuple); fan++ {
		comments = append(comments,"symmetrical nodes")
	}

	// The Tuple array contains the co-active members. Since these are probed in parallel
	// they belong to different/parallel proper timelines

	hub := S.CreateHub(*g,this.Key,"Symmetric service hub",1)

	for a := 0; a < len(this.Tuple); a++ {

		// Check for any hubs that also contain these addresses, as we may
		// need to join them rather than starting a new hub

		// Single IPs are Nodes - skip dark nodes *
		// Spacelike parallel slices are Hubs

		member := S.CreateNode(*g,this.Tuple[a],"IP node",1)
		S.CreateLink(*g, hub, "CONTAINS", member,1)

		hubs := HubThatContain(*g,this.Tuple[a])
		
		// If member of more than one group, mark them  as NEAR/CO-LOCATED
		// We can later run batch jobs to form even higher level agreggate hubs for efficiency
		
		for h1 := 0; h1 < len(hubs); h1++ {
			for h2 := h1 + 1; h2 < len(hubs); h2++ {
				
				fmt.Println("!!!Merging hubs",this.Key,hubs[h1],"NEAR",hubs[h2])

				S.CreateLink(*g,hubs[h1],"COACTIV",hubs[h2],1)
				S.CreateLink(*g,hubs[h2],"COACTIV",hubs[h1],1)
					
			}
		}
	}

	return this.Tuple, comments
}

//****************************************************

func HubThatContain(g S.Analytics, key string) []S.Node {

	querystring := "FOR my IN Contains FILTER my._to == \"Nodes/" + key + "\" RETURN DOCUMENT(my)"

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Neighbour query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	var result []S.Node
	
	for {
		var doc S.Node

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			if doc.Key != "" {
				result = append(result,doc)
			}
		}
	}

return result
}
