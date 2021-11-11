//
// Copyright © Mark Burgess
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// ***************************************************************************
//*
//* Cellibrium/SST for CAIDA data
//*
// ***************************************************************************

package SST

import (
	"strings"
	"context"
	"fmt"
	"path"
	"os"
	"hash/fnv"
	"time"
	"regexp"

	A "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

// ***************************************************************************
// Some globals
// ***************************************************************************

// Running memory of recent thinking for N-fragments up to 3

const INITIAL_VALUE = 1
const MAXCLUSTERS = 4
const MAX = 20

// ***************************************************************************
// Some datatypes
// ***************************************************************************

type Name string
type List []string
type Neighbours []int

// ****************************************************************************

type ConnectionSemantics struct {

	LinkType string  // Association type
	From     string  // Node key pointed to

	// Used in aggregation

	FwdSrc   string
	BwdSrc   string
}

type SemanticLinkSet map[string][]ConnectionSemantics

type Cone map[int]SemanticLinkSet

// ****************************************************************************

type ITDK struct {
	
	// Container db
	
	S_db   A.Database      // One for each spacelike snapshot, labelled by UTC
	
	// Graph model
	
	S_graph A.Graph
	
	// 3 levels of nodes and supernodes
	
	S_DNS       A.Collection
	S_AS        A.Collection
	S_Devices   A.Collection // (aka "nodes" in CAIDA speak)
	S_Unknown   A.Collection // hubs that bond "hyperlinks" or wormholes/tunnels (ether,MPLS, etc)
	S_Country   A.Collection
	S_Region    A.Collection
	S_IPv4      A.Collection
	S_IPv6      A.Collection

        // mobile IP

	// 4 primary link types in the graph
	
	S_Follows   A.Collection   // Unneeded
	S_Contains  A.Collection   // Country -> Region --> City --->AliasSet, DNS ---> IPv*
	S_Expresses A.Collection   // AliasSet -> IP, AS-->AliasSet
	S_Near      A.Collection   // Adjacency

	// Chain memory 
	previous_event_key Node
	previous_event_slice []Node
}

// ***************************************************************************

type ITDKSnapshot struct {
	
	Key         string
	StartSample time.Time 
	EndSample   time.Time 
	Snapshot    string    // database name of type ITDK
}

// ***************************************************************************

type IntKeyValue struct {

	K  string `json:"_key"`
	V  int    `json:"value"`
}

// ****************************************************************************

type Node struct {
	Key     string    `json:"_key"`

	prefix  string    // don't save, non exported with lower case(!)
	Comment string    `json:"data"`
	Weight  float64   `json:"weight"`
	Coords  [2]float64 `json:"coordinates"`
}

// ***************************************************************************

type Link struct {
	From        string `json:"_from"`     // mandatory field
	To          string `json:"_to"`       // mandatory field
	CommentFrom string `json:"NBfrom"`    // Comment about from node
	CommentTo   string `json:"NBto"  `    // Comment about to node
        SId         string `json:"semantics"` // Matches Association key
	Negate        bool `json:"negation"`  // is this enable or block?
	Weight     float64 `json:"weight"`
	Key         string `json:"_key"`      // mandatory field (handle)
}

// ****************************************************************************

// Use these to store invariant relationship data as look up tables
// this prevents the DB data from being larger than necessary.

type Association struct {

	Key     string    `json:"_key"`

	STtype  int       `json:"STType"`
	Fwd     string    `json:"Fwd"`
	Bwd     string    `json:"Bwd"` 
	NFwd    string    `json:"NFwd"`
	NBwd    string    `json:"NBwd"`
}

//**************************************************************

var CONST_STtype = make(map[string]int)
var ASSOCIATIONS = make(map[string]Association)
var STTYPES []IntKeyValue

const GR_NEAR int      = 1  // approx like
const GR_FOLLOWS int   = 2  // i.e. influenced by
const GR_CONTAINS int  = 3 
const GR_EXPRESSES int = 4  // represents, etc

//**************************************************************

type VectorPair struct {
	From string
	To string
}

//**************************************************************
// Set up the Arango
//**************************************************************

func InitializeSmartSpaceTime() {

	// - sign indicates arrow points in opposite direction to assumed semantics, e.g. "CONTAINS"

	ASSOCIATIONS["PART_OF"] = Association{"PART_OF",-GR_CONTAINS,"incorporates","is part of","is not part of","doesn't contribute to"}

	ASSOCIATIONS["DEVICE_IN"] = Association{"DEVICE_IN",-GR_CONTAINS,"is located in","contains","is not in","doesn't contain"}
	ASSOCIATIONS["REGION_IN"] = Association{"REGION_IN",-GR_CONTAINS,"is located in","contains","is not in","doesn't contain"}

	ASSOCIATIONS["HAS_INTERFACE"] = Association{"HAS_INTERFACE",GR_CONTAINS,"has interface address","is an interface address of","does not have interface","not an interface address of"}

	// *

	ASSOCIATIONS["HAS_ADDR"] = Association{"HAS_ADDR",GR_EXPRESSES,"has address","is an address of","does not have address","not an address of"}

	// *

	ASSOCIATIONS["ADJ_NODE"] = Association{"ADJ_NODE",GR_NEAR,"has route IP to","has IP route to","no IP route to","no route to"}

	ASSOCIATIONS["ADJ_UNKNOWN"] = Association{"ADJ_UNKNOWN",GR_NEAR,"connects through unknown","connects to device","does not connect through unknown","does not connect to device"}

	ASSOCIATIONS["ADJ_IP"] = Association{"ADJ_IP",GR_NEAR,"is IP adjacent to","is IP adjacent to","is not connected to","is not connected to"}

	// *

	ASSOCIATIONS["DERIVES_FROM"] = Association{"DERIVES_FROM",GR_FOLLOWS,"derives from","leads to","does not derive from","does not leadto"}
	ASSOCIATIONS["DEPENDS"] = Association{"DEPENDS",GR_FOLLOWS,"may depend on","may determine","doesn't depend on","doesn't determine"}
	ASSOCIATIONS["LEADS_TO"] = Association{"LEADS_TO",-GR_FOLLOWS,"leads to","doesn't imply","doen't reach","doesn't precede"}
	ASSOCIATIONS["PRECEDES"] = Association{"PRECEDES",-GR_FOLLOWS,"precedes","follows","doen't precede","doesn't precede"}


}

// ****************************************************************************
//  Graph invariants
// ****************************************************************************

func CreateLink(g ITDK, c1 Node, rel string, c2 Node, weight float64) {

	var link Link

	//fmt.Println("CreateLink: ",c1,"rel",rel,"c2",c2)

	link.From = c1.prefix + strings.ReplaceAll(c1.Key," ","_")
	link.To = c2.prefix + strings.ReplaceAll(c2.Key," ","_")
	link.SId = ASSOCIATIONS[rel].Key
	link.Weight = weight
	link.Negate = false

	if link.SId != rel {
		fmt.Println("Associations not set up -- missing InitializeSmartSpacecTime?",rel)
		os.Exit(1)
	}

	AddLink(g,link)
}

// ****************************************************************************

func CommentedLink(g ITDK, c1 Node, rel string, c2 Node, nbfr,nbto string, weight float64) {

	var link Link

	link.From = c1.prefix + strings.ReplaceAll(c1.Key," ","_")
	link.To = c2.prefix + strings.ReplaceAll(c2.Key," ","_")
	link.SId = ASSOCIATIONS[rel].Key
	link.Weight = weight
	link.Negate = false

	link.CommentFrom = nbfr
	link.CommentTo = nbto

	if link.SId != rel {
		fmt.Println("Associations not set up -- missing InitializeSmartSpacecTime?",rel)
		os.Exit(1)
	}

	//fmt.Println("Adding commentedLink:",link)

	AddLink(g,link)
}

// ****************************************************************************

func BlockLink(g ITDK, c1 Node, rel string, c2 Node, weight float64) {

	var link Link

	//fmt.Println("CreateLink: c1",c1,"rel",rel,"c2",c2)

	link.From = c1.prefix + strings.ReplaceAll(c1.Key," ","_")
	link.To = c2.prefix + strings.ReplaceAll(c2.Key," ","_")
	link.SId = ASSOCIATIONS[rel].Key
	link.Weight = weight
	link.Negate = true

	if link.SId != rel {
		fmt.Println("Associations not set up -- missing InitializeSmartSpacecTime?")
		os.Exit(1)
	}

	AddLink(g,link)
}

// ****************************************************************************

func IncrementLink(g ITDK, c1 Node, rel string, c2 Node) {

	var link Link

	//fmt.Println("IncremenLink: c1",c1,"rel",rel,"c2",c2)

	link.From = c1.prefix + c1.Key
	link.To = c2.prefix + c2.Key
	link.SId = ASSOCIATIONS[rel].Key

	IncrLink(g,link)
}

// ****************************************************************************

func CreateDevice(g ITDK, name string) Node {

	var device Node

	device.Key = name
	device.prefix = "Devices/"
	device.Comment = ""
	device.Weight = 0

	InsertNodeIntoCollection(g,device,g.S_Devices)
	return device
}

// ****************************************************************************

func CreateUnknown(g ITDK, name string) Node {

	var unknown Node

	unknown.Key = name
	unknown.prefix = "Unknown/"
	unknown.Comment = ""
	unknown.Weight = 0

	InsertNodeIntoCollection(g,unknown,g.S_Unknown)
	return unknown
}

// ****************************************************************************

func CreateIPv4(g ITDK, name string) Node {

	var ip Node

	ip.Key = name
	ip.prefix = "IPv4/"
	ip.Comment = ""
	ip.Weight = 0

	InsertNodeIntoCollection(g,ip,g.S_IPv4)
	return ip
}

// ****************************************************************************

func CreateIPv6(g ITDK, name string) Node {

	var ip Node

	ip.Key = name
	ip.prefix = "IPv6/"
	ip.Comment = ""
	ip.Weight = 0

	InsertNodeIntoCollection(g,ip,g.S_IPv6)
	return ip
}

// ****************************************************************************

func CreateAS(g ITDK, name, method string) Node {

	var as Node

	as.Key = name
	as.prefix = "AS/"
	as.Comment = method
	as.Weight = 0

	InsertNodeIntoCollection(g,as,g.S_AS)
	return as
}

// ****************************************************************************

func CreateRegion(g ITDK, shortname,fullname string, lat,long float64) Node {

	var region Node

	if fullname == "" {
		fullname = "Data error in file format"
	}

	if shortname == "" {
		shortname = strings.ReplaceAll(fullname," ","_")
	}

	region.Key = InvariantDescription(shortname)
	region.prefix = "Region/"
	region.Comment = fullname
	region.Weight = 0
	region.Coords[0] = long  // RFC 7946 Position [long,lat]
	region.Coords[1] = lat

	InsertNodeIntoCollection(g,region,g.S_Region)
	return region
}

// ****************************************************************************

func CreateCountry(g ITDK, name string) Node {

	var country Node

	country.Key = name
	country.prefix = "Country/"
	country.Comment = ""
	country.Weight = 0

	InsertNodeIntoCollection(g,country,g.S_Country)
	return country
}

// ****************************************************************************

func CreateDomain(g ITDK, name string) Node {

	var domain Node

	domain.Key = InvariantDescription(name)
	domain.prefix = "DNS/"
	domain.Comment = name
	domain.Weight = 0

	InsertNodeIntoCollection(g,domain,g.S_DNS)
	return domain
}

// ***************************************************************************

func InvariantDescription(s string) string {

	reg, err := regexp.Compile("[^a-zA-Z0-9]+")

	if err != nil {
		fmt.Println("Regex failed")
		os.Exit(1)
	}

	s = reg.ReplaceAllString(s, "_")

	return strings.Trim(s,"\n ")
}

// ****************************************************************************

func GetNode(g ITDK, key string) Node {

	var doc Node
	var prefix string
	var rawkey string
	var coll A.Collection

	prefix = path.Dir(key)
	rawkey = path.Base(key)

	//fmt.Println("Debug GetNode(key)",key," XXXX pref",prefix,"base",rawkey)

	switch prefix {

	case "DNS": 
		coll = g.S_DNS
		break

	case "AS": 
		coll = g.S_AS
		break

	case "Devices": 
		coll = g.S_Devices
		break

	case "Unknown": 
		coll = g.S_Unknown
		break

	case "Country": 
		coll = g.S_Country
		break

	case "Region": 
		coll = g.S_Region
		break

	case "IPv4": 
		coll = g.S_IPv4
		break

	case "IPv6": 
		coll = g.S_IPv6
		break

	default:
		fmt.Println("No such collection in GetNode")
		os.Exit(1)

	}

	// if we use S_nodes reference then we don't need the Nodes/ prefix

	_, err := coll.ReadDocument(nil, rawkey, &doc)

	if err != nil {
		fmt.Println("No such concept",err,rawkey)
		os.Exit(1)
	}

	return doc
}

//***********************************************************************
// Arango
//***********************************************************************

func OpenDatabase(name, url, user, pwd string) A.Database {

	var db A.Database
	var db_exists bool
	var err error
	var client A.Client

	ctx := context.Background()

	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ url },
	})

	if err != nil {
		fmt.Printf("Failed to create HTTP connection: %v", err)
	}

	client, err = A.NewClient(A.ClientConfig{
		Connection: conn,
		Authentication: A.BasicAuthentication(user, pwd),
	})

	db_exists, err = client.DatabaseExists(ctx,name)

	if db_exists {

		db, err = client.Database(ctx,name)

	} else {
		db, err = client.CreateDatabase(ctx,name, nil)
		
		if err != nil {
			fmt.Printf("Failed to create database: %v", err)
			os.Exit(1);
		}
	}

	return db
}

// ****************************************************************************

func fnvhash(b []byte) string { // Currently trusting this to have no collisions
        hash := fnv.New64a()
        hash.Write(b)
        h := hash.Sum64()
        return fmt.Sprintf("key_%d",h)
}

//***********************************************************************

func OpenITDK(dbname, service_url, user, pwd string) ITDK {

	var g ITDK
	var db A.Database

	InitializeSmartSpaceTime()

	db = OpenDatabase(dbname, service_url, user, pwd)

	// Book-keeping: wiring up edgeCollection to store the edges

	var F_edges A.EdgeDefinition
	var C_edges A.EdgeDefinition
	var N_edges A.EdgeDefinition
	var E_edges A.EdgeDefinition

	// Arango's cluster controller wants us to declare which links to search

	F_edges.Collection = "Follows"
	F_edges.From = []string{"Devices","AS"} // May not use this...
	F_edges.To = []string{"Devices","AS"}

	C_edges.Collection = "Contains"
	C_edges.From = []string{"Country","Region","Devices","Unknown","AS"}
	C_edges.To = []string{"Region","Devices","Unknown","IPv4","IPv6"}

	N_edges.Collection = "Near"
	N_edges.From = []string{"Devices","Unknown","IPv4","IPv6","AS"}
	N_edges.To = []string{"Devices","IPv4","IPv6","AS"}

	E_edges.Collection = "Expresses"
	E_edges.From = []string{"Devices","DNS","AS"}
	E_edges.To = []string{"IPv4","IPv6","DNS"}

	var options A.CreateGraphOptions
	options.OrphanVertexCollections = []string{"Disconnected"}
	options.EdgeDefinitions = []A.EdgeDefinition{F_edges,C_edges,N_edges,E_edges}

	// Begin - feed options into a graph 

	var graph A.Graph
	var err error
	var gname string = "concept_spacetime"
	var g_exists bool

	g_exists, err = db.GraphExists(nil, gname)

	if g_exists {
		graph, err = db.Graph(nil,gname)

		if err != nil {
			fmt.Printf("Open graph: %v", err)
			os.Exit(1)
		}

	} else {
		graph, err = db.CreateGraph(nil, gname, &options)

		if err != nil {
			fmt.Printf("Create graph: %v", err)
			os.Exit(1)
		}
	}

	// *** Nodes

	var dns_vertices A.Collection
	var as_vertices A.Collection
	var device_vertices A.Collection
	var country_vertices A.Collection
	var region_vertices A.Collection
	var ipv4_vertices A.Collection
	var ipv6_vertices A.Collection
	var unknown_vertices A.Collection

	dns_vertices, err = graph.VertexCollection(nil, "DNS")

	if err != nil {
		fmt.Printf("Vertex collection DNS: %v\n", err)
	}

	device_vertices, err = graph.VertexCollection(nil, "Devices")

	if err != nil {
		fmt.Printf("Vertex collection Devices: %v\n", err)
	}

	unknown_vertices, err = graph.VertexCollection(nil, "Unknown")

	if err != nil {
		fmt.Printf("Vertex collection Unknown: %v\n", err)
	}

	country_vertices, err = graph.VertexCollection(nil, "Country")

	if err != nil {
		fmt.Printf("Vertex collection Country: %v\n", err)
	}

	region_vertices, err = graph.VertexCollection(nil, "Region")

	if err != nil {
		fmt.Printf("Vertex collection Region: %v\n", err)
	}

	ipv4_vertices, err = graph.VertexCollection(nil, "IPv4")

	if err != nil {
		fmt.Printf("Vertex collection IPv4: %v\n", err)
	}

	ipv6_vertices, err = graph.VertexCollection(nil, "IPv6")

	if err != nil {
		fmt.Printf("Vertex collection IPv6: %v\n", err)
	}

	as_vertices, err = graph.VertexCollection(nil, "AS")

	if err != nil {
		fmt.Printf("Vertex collection AS: %v\n", err)
	}

	// *** Links

	var F_edgeset A.Collection
	var C_edgeset A.Collection
	var E_edgeset A.Collection
	var N_edgeset A.Collection

	F_edgeset, _, err = graph.EdgeCollection(nil, "Follows")

	if err != nil {
		fmt.Printf("Egdes follows: %v", err)
	}

	C_edgeset, _, err = graph.EdgeCollection(nil, "Contains")

	if err != nil {
		fmt.Printf("Edges contains: %v", err)
	}

	E_edgeset, _, err = graph.EdgeCollection(nil, "Expresses")

	if err != nil {
		fmt.Printf("Edges expresses: %v", err)
	}

	N_edgeset, _, err = graph.EdgeCollection(nil, "Near")

	if err != nil {
		fmt.Printf("Edges near: %v", err)
	}

	g.S_db = db	
	g.S_graph = graph

	g.S_DNS = dns_vertices
	g.S_AS = as_vertices
	g.S_Devices = device_vertices
	g.S_Unknown = unknown_vertices
	g.S_Country = country_vertices
	g.S_Region = region_vertices
	g.S_IPv4 = ipv4_vertices
	g.S_IPv6 = ipv6_vertices

	g.S_Follows = F_edgeset
	g.S_Contains = C_edgeset
	g.S_Expresses = E_edgeset	
	g.S_Near = N_edgeset

	g.previous_event_key = Node{ Key: "start" }

	return g
}

// **************************************************

func AddLinkCollection(g ITDK, name string, nodecoll string) A.Collection {

	var edgeset A.Collection
	var c A.VertexConstraints

	// Remember we have to define allowed source/sink constraints for edges

	c.From = []string{nodecoll}  // source set
	c.To = []string{nodecoll}    // sink set

	exists, err := g.S_graph.EdgeCollectionExists(nil, name)

	if !exists {
		edgeset, err = g.S_graph.CreateEdgeCollection(nil, name, c)
		
		if err != nil {
			fmt.Printf("Edge collection failed: %v\n", err)
		}
	}

return edgeset
}

// **************************************************

func AddNodeCollection(g ITDK, name string) A.Collection {

	var nodeset A.Collection

	exists, err := g.S_graph.VertexCollectionExists(nil, name)

	if !exists {
		nodeset, err = g.S_graph.CreateVertexCollection(nil, name)
		
		if err != nil {
			fmt.Printf("Node collection failed: %v\n", err)
		}
	}

return nodeset
}

// **************************************************

func InsertNodeIntoCollection(g ITDK, node Node, coll A.Collection) {
	
	exists,err := coll.DocumentExists(nil, node.Key)
	
	if !exists {
		_, err = coll.CreateDocument(nil, node)
		
		if err != nil {
			fmt.Println("Failed to create non existent node in InsertNodeIntoCollection: ",node,err)
			return
		}

	} else {

		// Don't need to check correct value, as each tuplet is unique, but check the data

		if node.Comment == "" && node.Weight == 0 {
			// Leave the values alone if we don't mean to update them
			return
		}
		
		var checknode Node

		_,err := coll.ReadDocument(nil,node.Key,&checknode)

		if err != nil {
			fmt.Printf("Failed to read value: %s %v",node.Key,err)
			return
		}

		if checknode != node {

			//fmt.Println("Correcting link values",checknode,"to",node)

			_, err := coll.UpdateDocument(nil, node.Key, node)

			if err != nil {
				fmt.Printf("Failed to update value: %s %v",node,err)
				return

			}
		}
	}
}

// **************************************************

func AddLink(g ITDK, link Link) {

	// Don't add multiple edges that are identical! But allow types

	 //fmt.Println("Checking link",link)

	// We have to make our own key to prevent multiple additions
        // - careful of possible collisions, but this should be overkill

        description := link.From + link.SId + link.To
	key := fnvhash([]byte(description))

	ass := ASSOCIATIONS[link.SId].Key

	if ass == "" {
		fmt.Println("Unknown association from link",link,"Sid",link.SId)
		os.Exit(1)
	}

	edge := Link{
 	 	From: link.From, 
		To: link.To, 
		SId: ass,
		Negate: link.Negate,
		Weight: link.Weight,
		CommentFrom: link.CommentFrom,
		CommentTo: link.CommentTo,
		Key: key,
	}

	var links A.Collection
	var coltype int

	// clumsy abs()

	if ASSOCIATIONS[link.SId].STtype < 0 {

		coltype = -ASSOCIATIONS[link.SId].STtype

	} else {

		coltype = ASSOCIATIONS[link.SId].STtype

	}

	switch coltype {
		
	case GR_FOLLOWS:   links = g.S_Follows
	case GR_CONTAINS:  links = g.S_Contains
	case GR_EXPRESSES: links = g.S_Expresses
	case GR_NEAR:      links = g.S_Near

	}

	exists,_ := links.DocumentExists(nil, key)

	if !exists {
		_, err := links.CreateDocument(nil, edge)
		
		if err != nil {
			fmt.Println("Failed to add new link (addlink):", err, "L:",link, "E:", edge)
			os.Exit(1);
		}
	} else {

		if edge.Weight < 0 {

			// Don't update if the weight is negative
			return
		}

		// Don't need to check correct value, as each tuplet is unique, but check the weight
		
		var checkedge Link

		_,err := links.ReadDocument(nil,key,&checkedge)

		if err != nil {
			fmt.Printf("Failed to read value: %s %v",key,err)
			os.Exit(1);	
		}

		if checkedge != edge {

			//fmt.Println("Correcting link",checkedge,"to",edge)

			_, err := links.UpdateDocument(nil, key, edge)

			if err != nil {
				fmt.Printf("Failed to update value: %s %v",edge,err)
				os.Exit(1);

			}
		}
	}
}

// **************************************************

func IncrLink(g ITDK, link Link) {

	// Don't add multiple edges that are identical! But allow types

	// fmt.Println("Checking link",link)

	// We have to make our own key to prevent multiple additions
        // - careful of possible collisions, but this should be overkill

        description := link.From + link.SId + link.To
	key := fnvhash([]byte(description))

	ass := ASSOCIATIONS[link.SId].Key

	if ass == "" {
		fmt.Println("Unknown association from link",link,"Sid",link.SId)
		os.Exit(1)
	}

	edge := Link{
 	 	From: link.From, 
		SId: ass,
		To: link.To, 
		Key: key,
		Weight: 0,
	}

	var links A.Collection
	var coltype int

	// clumsy abs()

	if ASSOCIATIONS[link.SId].STtype < 0 {

		coltype = -ASSOCIATIONS[link.SId].STtype

	} else {

		coltype = ASSOCIATIONS[link.SId].STtype

	}

	switch coltype {

	case GR_FOLLOWS:   links = g.S_Follows
	case GR_CONTAINS:  links = g.S_Contains
	case GR_EXPRESSES: links = g.S_Expresses
	case GR_NEAR:      links = g.S_Near

	}

	exists,_ := links.DocumentExists(nil, key)

	if !exists {
		_, err := links.CreateDocument(nil, edge)
		
		if err != nil {
			fmt.Println("Failed to add new link (incrlink):", err, link, edge)
			os.Exit(1);
		}
	} else {

		// Don't need to check correct value, as each tuplet is unique, but check the weight
		
		var checkedge Link

		_,err := links.ReadDocument(nil,key,&checkedge)

		if err != nil {
			fmt.Printf("Failed to read value: %s %v",key,err)
			os.Exit(1);	
		}

		edge.Weight = checkedge.Weight + 1.0

		//fmt.Println("updating",edge)
		
		_, err = links.UpdateDocument(nil, key, edge)
		
		if err != nil {
			fmt.Printf("Failed to update value: %s %v",edge,err)
			os.Exit(1);
			
		}
	}
}

// **************************************************

func PrintNodes(g ITDK, collection string) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + collection + " RETURN doc"

	cursor,err = g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var doc Node

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			fmt.Print(collection,doc,"\n")
		}
	}
}

// **************************************************

func GetSuccessorsOf(g ITDK, node string, sttype int) SemanticLinkSet {

	return GetNeighboursOf(g,node,sttype,"+")
}

// **************************************************

func GetPredecessorsOf(g ITDK, node string, sttype int) SemanticLinkSet {

	return GetNeighboursOf(g,node,sttype,"-")
}

// **************************************************

func GetNeighboursOf(g ITDK, node string, sttype int, direction string) SemanticLinkSet {

	var err error
	var cursor A.Cursor
	var coll string

	if !strings.Contains(node,"/") {
		fmt.Println("GetNeighboursOf(node) without collection prefix",node)
		os.Exit(1)
	}

	switch sttype {

	case -GR_FOLLOWS, GR_FOLLOWS:   
		coll = "Follows"

	case -GR_CONTAINS, GR_CONTAINS:  
		coll = "Contains"

	case -GR_EXPRESSES, GR_EXPRESSES: 
		coll = "Expresses"

	case -GR_NEAR, GR_NEAR:      
		coll = "Near"
	default:
		fmt.Println("Unknown STtype in GetNeighboursOf",sttype)
		os.Exit(1)
	}

	var querystring string

	switch direction {

	case "+": 
		querystring = "FOR my IN " + coll + " FILTER my._from == \"" + node + "\" RETURN my"
		break
	case "-":
		querystring = "FOR my IN " + coll + " FILTER my._to == \"" + node + "\"  RETURN my"
		break
	default:
		fmt.Println("NeighbourOf direction can only be + or -")
		os.Exit(1)
	}

	//fmt.Println("query:",querystring)

	cursor,err = g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Neighbour query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	var result SemanticLinkSet = make(SemanticLinkSet)

	for {
		var doc Link
		var nodekey string
		var linktype ConnectionSemantics

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			switch direction {

			case "-": 
				nodekey = doc.From
				linktype.From = doc.To
				linktype.LinkType = ASSOCIATIONS[doc.SId].Bwd
				break
			case "+":
				nodekey = doc.To
				linktype.From = doc.From
				linktype.LinkType = ASSOCIATIONS[doc.SId].Fwd
				break
			}

			result[nodekey] = append(result[nodekey],linktype)
		}
	}

	return result
}

// ********************************************************************

func GetAdjacencyMatrixByKey(g ITDK, assoc_type string, symmetrize bool) map[VectorPair]float64 {

	var adjacency_matrix = make(map[VectorPair]float64)

	var err error
	var cursor A.Cursor
	var coll string

	sttype := ASSOCIATIONS[assoc_type].STtype

	switch sttype {

	case -GR_FOLLOWS, GR_FOLLOWS:   
		coll = "Follows"

	case -GR_CONTAINS, GR_CONTAINS:  
		coll = "Contains"

	case -GR_EXPRESSES, GR_EXPRESSES: 
		coll = "Expresses"

	case -GR_NEAR, GR_NEAR:      
		coll = "Near"

	default:
		fmt.Println("Unknown STtype in GetNeighboursOf",assoc_type)
		os.Exit(1)
	}

	var querystring string

	querystring = "FOR my IN " + coll + " FILTER my.semantics == \"" + assoc_type + "\" RETURN my"

	cursor,err = g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Neighbour query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	for {
		var doc Link

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			if sttype == GR_NEAR || symmetrize {
				adjacency_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
				adjacency_matrix[VectorPair{From: doc.To, To: doc.From }] = 1.0
			} else {
				adjacency_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
			}
		}
	}

return adjacency_matrix
}

// ********************************************************************

func GetAdjacencyMatrixByInt(g ITDK, assoc_type string, symmetrize bool) ([][]float64,int,map[int]string) {

	var key_matrix = make(map[VectorPair]float64)

	var err error
	var cursor A.Cursor
	var coll string

	sttype := ASSOCIATIONS[assoc_type].STtype

	switch sttype {

	case -GR_FOLLOWS, GR_FOLLOWS:   
		coll = "Follows"

	case -GR_CONTAINS, GR_CONTAINS:  
		coll = "Contains"

	case -GR_EXPRESSES, GR_EXPRESSES: 
		coll = "Expresses"

	case -GR_NEAR, GR_NEAR:      
		coll = "Near"

	default:
		fmt.Println("Unknown STtype in GetNeighboursOf",assoc_type)
		os.Exit(1)
	}

	var querystring string

	querystring = "FOR my IN " + coll + " FILTER my.semantics == \"" + assoc_type + "\" RETURN my"

	cursor,err = g.S_db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Neighbour query \"%s\"failed: %v", querystring,err)
	}

	defer cursor.Close()

	var sets = make(Set)

	for {
		var doc Link

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {

			// Merge an idempotent list of nodes to find int address

			TogetherWith(sets,"adj",doc.To)
			TogetherWith(sets,"adj",doc.From)

			if sttype == GR_NEAR || symmetrize {
				key_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
				key_matrix[VectorPair{From: doc.To, To: doc.From }] = 1.0
			} else {
				key_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
			}
		}
	}

	//fmt.Println(sets)

	dimension := len(sets["adj"])
	var adjacency_matrix = make([][]float64,dimension)
	var keys = make(map[int]string)
	var i int = 0
	var j int = 0

	for ri := range sets["adj"] {

		adjacency_matrix[i] = make([]float64,dimension)
		keys[i] = sets["adj"][ri]

		for rj := range sets["adj"] {

			if key_matrix[VectorPair{From: sets["adj"][ri], To: sets["adj"][rj]}] > 0 {
				adjacency_matrix[i][j] = 1.0
			}
			j++
		}
		i++
	}

	return adjacency_matrix, dimension, keys
}

//*************************************************************

func GetFullAdjacencyMatrix(g ITDK, symmetrize bool) ([][]float64,int,map[int]string) {

	var key_matrix = make(map[VectorPair]float64)
	var sets = make(Set)

	var err error
	var cursor A.Cursor

	var STtypes []string = []string{ "Follows", "Contains", "Expresses", "Near" }

	for coll := range STtypes {

		var querystring string

		querystring = "FOR my IN " + STtypes[coll] + " RETURN my"
		
		cursor,err = g.S_db.Query(nil,querystring,nil)
		
		if err != nil {
			fmt.Printf("Full adjacency query \"%s\"failed: %v", querystring,err)
		}
		
		defer cursor.Close()
		
		for {
			var doc Link
			
			_,err = cursor.ReadDocument(nil,&doc)
			
			if A.IsNoMoreDocuments(err) {
				break
			} else if err != nil {
				fmt.Printf("Doc returned: %v", err)
			} else {

				// Merge an idempotent list of nodes to find int address
				
				TogetherWith(sets,"adj",doc.To)
				TogetherWith(sets,"adj",doc.From)
				
				if symmetrize {
					key_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
					key_matrix[VectorPair{From: doc.To, To: doc.From }] = 1.0
				} else {
					key_matrix[VectorPair{From: doc.From, To: doc.To }] = 1.0
				}
			}
		}
	}

	//fmt.Println(sets)

	dimension := len(sets["adj"])
	var adjacency_matrix = make([][]float64,dimension)
	var keys = make(map[int]string)
	var i int = 0
	var j int = 0

	for ri := range sets["adj"] {

		adjacency_matrix[i] = make([]float64,dimension)
		keys[i] = sets["adj"][ri]

		for rj := range sets["adj"] {

			if key_matrix[VectorPair{From: sets["adj"][ri], To: sets["adj"][rj]}] > 0 {
				adjacency_matrix[i][j] = 1.0
			}
			j++
		}
		i++
	}

	return adjacency_matrix, dimension, keys
}

//**************************************************************

func PrintMatrix(adjacency_matrix [][]float64,dim int,keys map[int]string) {

	for i := 1; i < dim; i++ {

		fmt.Printf("%12.12s: ",keys[i])

		for j := 1; j < dim; j++ {
			fmt.Printf("%3.3f ",adjacency_matrix[i][j])
		}
		fmt.Println("")
	}
}

//**************************************************************

func PrintVector (vec []float64,dim int,keys map[int]string) {

	for i := 1; i < dim; i++ {
		
		fmt.Printf("%12.12s: ",keys[i])
		fmt.Printf("%3.3f \n",vec[i])
	}
}

//**************************************************************

func GetPrincipalEigenvector(adjacency_matrix [][]float64, dim int) []float64 {

	var ev = make([]float64,dim)
	var sum float64 = 0

	// start with a uniform positive value

	for i := 1; i < dim; i++ {
		ev[i] = 1.0
	}

	// Three iterations is probably enough .. could improve on this

	ev = MatrixMultiplyVector(adjacency_matrix,ev,dim)
	ev = MatrixMultiplyVector(adjacency_matrix,ev,dim)
	ev = MatrixMultiplyVector(adjacency_matrix,ev,dim)

	for i := 1; i < dim; i++ {
		sum += ev[i]
	}

	// Normalize vector

	if sum == 0 {
		sum = 1.0
	}

	for i := 1; i < dim; i++ {
		ev[i] = ev[i] / sum
	}

	return ev
}

//**************************************************************

func MatrixMultiplyVector(adj [][]float64,v []float64,dim int) []float64 {

	var result = make([]float64,dim)

	// start with a uniform positive value

	for i := 1; i < dim; i++ {

		result[i] = 0

		for j := 1; j < dim; j++ {

			result[i] = result[i] + adj[i][j] * v[j]
		}
	}

return result
}

//**************************************************************

func GetPossibilityCone(g ITDK, start_key string, sttype int, visited map[string]bool) (Cone,int) {

	// A cone is a sequence of spacelike slices orthogonal to the proper time defined by sttype
	// Each slice is formed from patches that spread from nodes in the current slice
	
	// width first

	var layer int = 0
	var counter int = 0
	var total int = 0
	var cone = make(Cone)

	var start string = start_key

	cone[layer] = InitializeSemanticLinkSet(start)

	for {		
		var fanout SemanticLinkSet

		cone[layer+1] = make(SemanticLinkSet)

		for nodekey := range cone[layer] {

			if visited[nodekey] {
				continue
			} else {
				visited[nodekey] = true
			}

			fanout = GetSuccessorsOf(g, nodekey, sttype)
			
			if len(fanout) == 0 {
				return cone,total
			}

			//fmt.Println(counter, "Successor", nodekey,"result", fanout)
						
			for nextkey := range fanout {

				for wire := range fanout[nextkey] {
					
					fanout[nextkey][wire].FwdSrc = nextkey

					if !AlreadyLinkType(cone[layer+1][nextkey],fanout[nextkey][wire]) {

						cone[layer+1][nextkey] = append(cone[layer+1][nextkey],fanout[nextkey][wire])
					}
				}

				//fmt.Println("Debug",counter,nextkey,fanout[nextkey])				
				counter = counter + 1
			}
		}
		
		layer = layer + 1
		total = total + counter
		counter = 0
	}
}

// **************************************************

func AlreadyLinkType(existing []ConnectionSemantics, newlnk ConnectionSemantics) bool {

	for e := range existing {

		if newlnk.LinkType == existing[e].LinkType {
			return true
		}
	}

return false
}

// **************************************************

func GetConePaths(g ITDK, start_key string, sttype int, visited map[string]bool) []string {

	// A cone is a sequence of spacelike slices orthogonal to the proper time defined by sttype
	// Each slice is formed from patches that spread from nodes in the current slice
	
	// width first

	var layer int = 0

	paths := GetPathsFrom(g, layer, start_key, sttype, visited)
	return paths
}

// **************************************************

func GetPathsFrom(g ITDK, layer int, startkey string, sttype int, visited map[string]bool) []string {

	// return a path starting from startkey

	var paths []string

	var fanout SemanticLinkSet

	// opendir()

	fanout = GetSuccessorsOf(g, startkey, sttype)
	
	if len(fanout) == 0 {
		return nil
	}
	
	// (readdir())
	for nextkey := range fanout {

		// Get the previous mixed link state
		
		var mixed_link string = ":("
	
		// join multiple linknames pointing to nextkey

		for linktype := range fanout[nextkey] {
			
			if len(mixed_link) > 2 {
				mixed_link = mixed_link + " or "
			}
			
			mixed_link = mixed_link + fanout[nextkey][linktype].LinkType
		}
		
		mixed_link = mixed_link + "):"

		prefix:= startkey + mixed_link

		// Then look for postfix children - depth first
		// which returns a string starting from nextkey
	
		subdir := GetPathsFrom(g,layer+1,nextkey,sttype,visited)
		
		for subpath := 0; subpath < len(subdir); subpath++ {

			paths = append(paths,prefix + subdir[subpath])
		}

		if len(subdir) == 0 {
			
			paths = append(paths,prefix + nextkey + ":(end)")
		}
	}

	return paths
}

// **************************************************

func InitializeSemanticLinkSet(start string) SemanticLinkSet {
	
	var startlink SemanticLinkSet = make(SemanticLinkSet)
	startlink[start] = []ConnectionSemantics{ ConnectionSemantics{From: "nothing"}}
	return startlink
}

// **************************************************

func SaveAssociations(collname string, db A.Database, kv map[string]Association) {

	// Create collection

	var err error
	var coll_exists bool
	var coll A.Collection

	coll_exists, err = db.CollectionExists(nil, collname)

	if coll_exists {
		fmt.Println("Collection " + collname +" exists already")

		coll, err = db.Collection(nil, collname)

		if err != nil {
			fmt.Printf("Existing collection: %v", err)
			os.Exit(1)
		}

	} else {

		coll, err = db.CreateCollection(nil, collname, nil)

		if err != nil {
			fmt.Printf("Failed to create collection: %v", err)
		}
	}

	for k := range kv {

		AddAssocKV(coll, k, kv[k])
	}
}

// **************************************************

func LoadAssociations(db A.Database, coll_name string) map[string]Association {

	assocs := make(map[string]Association)

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + coll_name +" LIMIT 1000 RETURN doc"

	cursor,err = db.Query(nil,querystring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var assoc Association

		_,err = cursor.ReadDocument(nil,&assoc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Assoc returned: %v", err)
		} else {
			assocs[assoc.Key] = assoc
		}
	}

	return assocs
}

// **************************************************

func AddAssocKV(coll A.Collection, key string, assoc Association) {

	// Add data with convergent semantics, CFEngine style

	exists,err := coll.DocumentExists(nil, key)

	if !exists {

		fmt.Println("Adding/Restoring",assoc)
		_, err = coll.CreateDocument(nil, assoc)
		
		if err != nil {
			fmt.Printf("Failed to create non existent node: %s %v",key,err)
			os.Exit(1);
		}
	} else {

		var checkassoc Association
		
		_,err = coll.ReadDocument(nil,key,&checkassoc)

		if checkassoc != assoc {
			fmt.Println("Correcting data",checkassoc,"to",assoc)
			_, err := coll.UpdateDocument(nil, key, assoc)
			if err != nil {
				fmt.Printf("Failed to update value: %s %v",assoc,err)
				os.Exit(1);

			}
		}
	}
}



// ****************************************************************************
// Set/Collection Aggregation - two versions using hashing/lists, which faster?
// ****************************************************************************

type Set map[string]map[string]string
type LinSet map[string][]string

// ****************************************************************************

func BelongsToSet(sets Set,member string) (bool,string,string) {

	// Generate the formatted superset of all nodes that contains "member" within it
	
	for s := range sets {
		if sets[s][member] == member {
			var list string
			for l := range sets[s] {
				list = list + sets[s][l] + ","
			}
			return true,"super-"+s,list
		}
	}
	
	return false,"",""
}

// ****************************************************************************

func TogetherWith(sets Set, a1,a2 string) {

	// Place a1 and s2 into the same set, growing the sets if necessary
	// i.e. gradual accretion of sets by similarity of a1 and a2, we use
	// maps (hashes) so no linear searching as lists get big

	var s1,s2 string

	var got1 bool = false
	var got2 bool = false

	for s := range sets {

		if sets[s][a1] == a1 {
			s1 = s
			got1 = true
		}
			
		if sets[s][a2] == a2 {
			s2 = s
			got2 = true
		}

		if got1 && got2 {
			break
		}
	}

	if got1 && got2 {

		if s1 == s2 {
			
			return        // already ok
			
		} else {
			// merge two sets - this might be a mistake when data are big
			// would like to just move a tag somehow, but still the search time
			// has to grow as the clusters cover more data

			// Since this is time consuming, move the smaller set

			l1 := len(sets[s1])
			l2 := len(sets[s2])

			if (l1 <= l2) {
				for m := range sets[s1] {
					sets[s2][m] = sets[s1][m]
				}
				delete(sets,s1)
			} else {
				for m := range sets[s2] {
					sets[s1][m] = sets[s2][m]
				}
				delete(sets,s2)
			}

			return
		}
	} 

	if got1 { // s1 is the home
		sets[s1][a2] = a2
		return
	}

	if got2 { // s2 is the home
		sets[s2][a1] = a1
		return
	}

	// new pair, pick a key

	sets[a1] = make(map[string]string)
	sets[a2] = make(map[string]string)

	sets[a1][a1] = a1
	sets[a1][a2] = a2

}

// ****************************************************************************
// Linearized version
// ****************************************************************************

func LinTogetherWith(sets LinSet, a1,a2 string) {

	var s1,s2 string

	var got1 bool = false
	var got2 bool = false

	for s := range sets {

		for m:= range sets[s] {
			if sets[s][m] == a1 {
				s1 = s
				got1 = true
			}
			
			if sets[s][m] == a2 {
				s2 = s
			got2 = true
			}
		}
		
	}

	if got1 && got2 {

		if s1 == s2 {
			
			return        // already ok
			
		} else {
			// merge two sets

			l1 := len(sets[s1])
			l2 := len(sets[s2])

			if (l1 <= l2) {
				for m := range sets[s1] {
					sets[s2] = append(sets[s2],sets[s1][m])
				}
				delete(sets,s1)
			} else {
				for m := range sets[s1] {
					sets[s1] = append(sets[s1],sets[s2][m])
				}
				delete(sets,s2)
			}

			return
		}
	} 

	if got1 { // s1 is the home
		sets[s1] = append(sets[s1],a2)
		return
	}

	if got2 { // s2 is the home
		sets[s2] = append(sets[s2],a1)
		return
	}

	// new pair, pick a key

	sets[a1] = append(sets[a1],a1)
	sets[a1] = append(sets[a1],a2)

}

// ****************************************************************************

func BelongsToLinSet(sets LinSet,member string) (bool,string,string) {

	for s := range sets {
		for m := range sets[s] {
			if member == sets[s][m] {
				var list string
				for l := range sets[s] {
					list = list + sets[s][l] + ","
				}
				return true,"super-"+s,list
			}
		}
	}

	return false,"",""
}

