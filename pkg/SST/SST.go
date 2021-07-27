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
//* Cellibrium v3 in golang - on Arango
//*
// ***************************************************************************

package SST

import (
	"strings"
	"strconv"
	"bufio"
	"context"
	"fmt"
	"log"
	"path"
	"os"
	// Try this for local string -> int
	"hash/fnv"

	A "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

// ***************************************************************************
// Some globals
// ***************************************************************************

var ANOMALY_POLICY_LIMIT int = 5 // min number of calls between anomaly reporting

// Running memory of recent thinking for N-fragments up to 3

const INITIAL_VALUE = 1
const MAXCLUSTERS = 4
const MAX = 20

// ***************************************************************************
// For the document scanning application
// ***************************************************************************

var STM_CONTEXT [MAXCLUSTERS][]string
var STM_CONTEXT_RANK map[string]float64

// ***************************************************************************
// Some datatypes
// ***************************************************************************

type Name string
type List []string
type Neighbours []int

// ****************************************************************************

type ConnectionSemantics struct {

	LinkType string
	From     string

	// Used in aggregation

	FwdSrc   string
	BwdSrc   string
}

type SemanticLinkSet map[string][]ConnectionSemantics

type Cone map[int]SemanticLinkSet

// ****************************************************************************

type Analytics struct {

// Container db

S_db   A.Database

// Graph model

S_graph A.Graph

// 3 levels of nodes and supernodes

S_frags A.Collection  // fractionated Ngrams
S_nodes A.Collection  // whole semantic events
S_hubs  A.Collection  // collective patterns

// 4 primary link types

S_Follows   A.Collection
S_Contains  A.Collection
S_Expresses A.Collection
S_Near      A.Collection

// Chain memory 
previous_event_key Node
}

// ************************************************************

type IntKeyValue struct {

	K  string `json:"_key"`
	V  int    `json:"value"`
}

// ****************************************************************************

type Node struct {
	Key     string `json:"_key"`     // mandatory field (handle) - short name
	Data    string `json: "data"`    // bulk data
	Prefix  string
	Weight float64 `json:"weight"`
}

// Go into collections labelled by CONST_STtype[]

type Link struct {
	From     string `json:"_from"`     // mandatory field
	To       string `json:"_to"`       // mandatory field
        SId      string `json:"semantics"` // Matches Association key
	Weight  float64 `json:"weight"`
	Key      string `json:"_key"`      // mandatory field (handle)
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

var CONST_STtype = make(map[string]int)
var ASSOCIATIONS = make(map[string]Association)
var STTYPES []IntKeyValue

const GR_NEAR int      = 1  // approx like
const GR_FOLLOWS int   = 2  // i.e. influenced by
const GR_CONTAINS int  = 3 
const GR_EXPRESSES int = 4  // represents, etc


//**************************************************************
// Set up the Arango
//**************************************************************

func InitializeSmartSpaceTime() {

	// Initialize converge association name lookups 

	STTYPES = []IntKeyValue{
		IntKeyValue{
			K: "NEAR",
			V:  GR_NEAR,
		},

		IntKeyValue{
			K: "FOLLOWS",
			V:  GR_FOLLOWS,
		},

		IntKeyValue{
			K: "CONTAINS",
			V:  GR_CONTAINS,
		},

		IntKeyValue{
			K: "EXPRESSES",
			V:  GR_EXPRESSES,
		},

	}

	//SaveIntKVMap("ST_Types",PC.S_db,STTYPES)
	//PrintIntKV(PC.S_db,"ST_Types")
	//LoadIntKV2Map(PC.S_db,"ST_Types", CONST_STtype)

	// ***********************************************

	// first element needs to be there to store the lookup key
	// second element stored as int to save space

	ASSOCIATIONS["CONTAINS"] = Association{"CONTAINS",GR_CONTAINS,"contains","belongs to or is part of","does not contain","is not part of"}

	ASSOCIATIONS["GENERALIZES"] = Association{"GENERALIZES",GR_CONTAINS,"generalizes","is a special case of","is not a generalization of","is not a special case of"}

	// reversed case of containment semantics

	ASSOCIATIONS["PART_OF"] = Association{"PART_OF",-GR_CONTAINS,"incorporates","is part of","is not part of","doesn't contribute to"}

	// *

	ASSOCIATIONS["HAS_ROLE"] = Association{"HAS_ROLE",GR_EXPRESSES,"has the role of","is a role fulfilled by","has no role","is not a role fulfilled by"}

	ASSOCIATIONS["ORIGINATES_FROM"] = Association{"ORIGINATES_FROM",CONST_STtype["FOLLOWS"],"originates from","is the source/origin of","does not originate from","is not the source/origin of"}

	ASSOCIATIONS["EXPRESSES"] = Association{"EXPRESSES",GR_EXPRESSES,"expresses an attribute","is an attribute of","has no attribute","is not an attribute of"}

	ASSOCIATIONS["PROMISES"] = Association{"PROMISES",GR_EXPRESSES,"promises/intends","is intended/promised by","rejects/promises to not","is rejected by"}

	ASSOCIATIONS["HAS_NAME"] = Association{"HAS_NAME",GR_EXPRESSES,"has proper name","is the proper name of","is not named","isn't the proper name of"}

	// *

	ASSOCIATIONS["FOLLOWS_FROM"] = Association{"FOLLOWS_FROM",GR_FOLLOWS,"follows on from","is followed by","does not follow","does not precede"}

	ASSOCIATIONS["USES"] = Association{"USES",GR_FOLLOWS,"uses","is used by","does not use","is not used by"}

	ASSOCIATIONS["CAUSEDBY"] = Association{"CAUSEDBY",GR_FOLLOWS,"caused by","may cause","was not caused by","probably didn't cause"}

	ASSOCIATIONS["DERIVES_FROM"] = Association{"DERIVES_FROM",GR_FOLLOWS,"derives from","leads to","does not derive from","does not leadto"}

	ASSOCIATIONS["DEPENDS"] = Association{"DEPENDS",GR_FOLLOWS,"may depend on","may determine","doesn't depend on","doesn't determine"}

	// Neg

	ASSOCIATIONS["NEXT"] = Association{"NEXT",-GR_FOLLOWS,"comes before","comes after","is not before","is not after"}

	ASSOCIATIONS["LEADS_TO"] = Association{"LEADS_TO",-GR_FOLLOWS,"leads to","doesn't imply","doen't reach","doesn't precede"}

	ASSOCIATIONS["PRECEDES"] = Association{"PRECEDES",-GR_FOLLOWS,"precedes","follows","doen't precede","doesn't precede"}

	// *

	ASSOCIATIONS["RELATED"] = Association{"RELATED",GR_NEAR,"may be related to","may be related to","likely unrelated to","likely unrelated to"}

	ASSOCIATIONS["ALIAS"] = Association{"ALIAS",GR_NEAR,"also known as","also known as","not known as","not known as"}

	ASSOCIATIONS["IS_LIKE"] = Association{"IS_LIKE",GR_NEAR,"is similar to","is similar to","is unlike","is unlike"}

	// *

	//MakeAssociations("ST_Associations",PC.S_db,ASSOCIATIONS)
	//newassociations := LoadAssociations(PC.S_db,"ST_Associations")

	//fmt.Println(newassociations)

}

// ****************************************************************************
//  Graph invariants
// ****************************************************************************

func NodeLink(g Analytics, c1 Node, rel string, c2 Node, weight float64) {

	var link Link

	//fmt.Println("NodeLink: c1",c1,"rel",rel,"c2",c2)

	link.From = c1.Prefix + c1.Key
	link.To = c2.Prefix + c2.Key
	link.SId = ASSOCIATIONS[rel].Key
	link.Weight = weight

	if link.SId != rel {
		fmt.Println("Associations not set up -- missing InitializeSmartSpacecTime?")
		os.Exit(1)
	}

	AddLink(g,link)
}

// ****************************************************************************

func CreateFragment(g Analytics, short_description,vardescription string) Node {

	var concept Node
// 	var err error

	// if no short description, use a hash of the data

	description := InvariantDescription(vardescription)

	concept.Data = description
	concept.Key = short_description             // _id
	concept.Prefix = "Fragments/"

	AddFrag(g,concept)

	return concept
}

// ****************************************************************************

func CreateNode(g Analytics, short_description,vardescription string, weight float64) Node {

	var concept Node
// 	var err error

	// if no short description, use a hash of the data

	description := InvariantDescription(vardescription)

	concept.Data = description
	concept.Key = short_description
	concept.Prefix = "Nodes/"
	concept.Weight = weight

	AddNode(g,concept)

	return concept
}

// ****************************************************************************

func CreateHub(g Analytics, short_description,vardescription string) Node {

	var concept Node
// 	var err error

	description := InvariantDescription(vardescription)

	concept.Data = description
	concept.Key = "Hub:" + short_description             // _id
	concept.Prefix = "Hubs/"

//	db.AddHub()

	return concept
}

//**************************************************************

func InvariantDescription(s string) string {

	return strings.Trim(s,"\n ")
}

// ****************************************************************************
// Event Histoyy
// ****************************************************************************

func NextDataEvent(g Analytics, shortkey, data string) Node {

	key  := CreateNode(g, shortkey, data, 1.0)   // selection #n

	if  (Node{}) != g.previous_event_key  {
		
		NodeLink(g, key,"FOLLOWS_FROM",g.previous_event_key, 1.0)
	}

	g.previous_event_key = key
	return key 
}

//**************************************************************

func GetNode(g Analytics, key string) string {

	var doc Node
	var prefix string
	var rawkey string
	var coll A.Collection

	prefix = path.Dir(key)
	rawkey = path.Base(key)

	//fmt.Println("Debug GetNode(key)",key," XXXX pref",prefix,"base",rawkey)

	switch prefix {

	case "Fragments": 
		coll = g.S_frags
		break

	default:
		coll = g.S_nodes
		break


	}

	// if we use S_nodes reference then we don't need the Nodes/ prefix

	_, err := coll.ReadDocument(nil, rawkey, &doc)

	if err != nil {
		fmt.Println("No such concept",err,rawkey)
		os.Exit(1)
	}

	return doc.Data
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
		log.Fatalf("Failed to create HTTP connection: %v", err)
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
			log.Fatalf("Failed to create database: %v", err)
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

//********************************************************

func UpdateHistogram(g Analytics, histoname, data string) {

	// Sanitize key - no spaces

	keyname := strings.ReplaceAll(data," ","_")

	// Check/Create collection

	var err error
	var coll_exists bool
	var coll A.Collection

	coll_exists, err = g.S_db.CollectionExists(nil, histoname)

	if coll_exists {
		fmt.Println("Histogram KV Collection " + histoname +" exists already")

		coll, err = g.S_db.Collection(nil, histoname)

		if err != nil {
			log.Fatalf("Existing collection: %v", err)
			os.Exit(1)
		}

	} else {

		coll, err = g.S_db.CreateCollection(nil, histoname, nil)

		if err != nil {
			log.Fatalf("Failed to create collection: %v", err)
		}


	}

	exists,err := coll.DocumentExists(nil, keyname)

	if !exists {

		var kv IntKeyValue

		kv.K = keyname
		kv.V = 1

		_, err = coll.CreateDocument(nil, kv)
		
		if err != nil {
			log.Fatalf("Failed to create non existent node: %s %v",kv.K,err)
			os.Exit(1);
		}
		return
	}

	IncrementIntKV(g, histoname, keyname)
}

//********************************************************

func SaveIntKVMap(collname string, db A.Database, kv []IntKeyValue) {

	// Create collection

	var err error
	var coll_exists bool
	var coll A.Collection

	coll_exists, err = db.CollectionExists(nil, collname)

	if coll_exists {
		fmt.Println("Collection " + collname +" exists already")

		coll, err = db.Collection(nil, collname)

		if err != nil {
			log.Fatalf("Existing collection: %v", err)
			os.Exit(1)
		}

	} else {

		coll, err = db.CreateCollection(nil, collname, nil)

		if err != nil {
			log.Fatalf("Failed to create collection: %v", err)
		}
	}

	for k := range kv {

		AddIntKV(coll, kv[k])
	}
}

// **************************************************

func PrintIntKV(db A.Database, coll_name string) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + coll_name +" LIMIT 10 RETURN doc"

	cursor,err = db.Query(nil,querystring,nil)

	if err != nil {
		log.Fatalf("Query \""+ querystring +"\" failed: %v", err)
		return
	}

	defer cursor.Close()

	for {
		var kv IntKeyValue

		metadata,err := cursor.ReadDocument(nil,&kv)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("KV returned: %v", err)
		} else {
			
			fmt.Print("debug (K,V): (",kv.K,",", kv.V,")    ....    (",metadata,")\n")
		}
	}
}

// **************************************************

func AddIntKV(coll A.Collection, kv IntKeyValue) {

	// Add data with convergent semantics, CFEngine style

	exists,err := coll.DocumentExists(nil, kv.K)

	if !exists {

		fmt.Println("Adding/Restoring",kv)
		_, err = coll.CreateDocument(nil, kv)
		
		if err != nil {
			log.Fatalf("Failed to create non existent node: %s %v",kv.K,err)
			os.Exit(1);
		}
	} else {

		var checkkv IntKeyValue
		
		_,err = coll.ReadDocument(nil,kv.K,&checkkv)

		if checkkv.V != kv.V {
			fmt.Println("Correcting data",checkkv,"to",kv)
			_, err := coll.UpdateDocument(nil, kv.K, kv)
			if err != nil {
				log.Fatalf("Failed to update value: %s %v",kv.K,err)
				os.Exit(1);
			}
		}
	}
}

// **************************************************

func IncrementIntKV(g Analytics, coll_name, key string) {

        // UPDATE doc WITH { karma: doc.karma + 1 } IN users

	querystring := "LET doc = DOCUMENT(\"" + coll_name + "/" + key + "\")\nUPDATE doc WITH { value: doc.value + 1 } IN " + coll_name

	cursor,err := g.S_db.Query(nil,querystring,nil)

	if err != nil {
		log.Fatalf("Query \""+ querystring +"\" failed: %v", err)
	}

	cursor.Close()
}

// **************************************************

func LoadIntKV2Map(db A.Database, coll_name string, extkv map[string]int) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN " + coll_name +" LIMIT 10 RETURN doc"

	cursor,err = db.Query(nil,querystring,nil)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var kv IntKeyValue

		_,err = cursor.ReadDocument(nil,&kv)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("KV returned: %v", err)
		} else {
			extkv[kv.K] = kv.V
		}
	}
}

//***********************************************************************

func OpenAnalytics(dbname, service_url, user, pwd string) Analytics {

	var g Analytics
	var db A.Database

	InitializeSmartSpaceTime()

	db = OpenDatabase(dbname, service_url, user, pwd)

	// Book-keeping: wiring up edgeCollection to store the edges

	var F_edges A.EdgeDefinition
	var C_edges A.EdgeDefinition
	var N_edges A.EdgeDefinition
	var E_edges A.EdgeDefinition

	F_edges.Collection = "Follows"
	F_edges.From = []string{"Nodes","Hubs","Fragments"}  // source set
	F_edges.To = []string{"Nodes","Hubs","Fragments"}    // sink set

	C_edges.Collection = "Contains"
	C_edges.From = []string{"Nodes","Hubs"}              // source set
	C_edges.To = []string{"Nodes","Hubs","Fragments"}    // sink set

	N_edges.Collection = "Near"
	N_edges.From = []string{"Nodes","Hubs","Fragments"}  // source set
	N_edges.To = []string{"Nodes","Hubs","Fragments"}    // sink set

	E_edges.Collection = "Expresses"
	E_edges.From = []string{"Nodes","Hubs"}  // source set
	E_edges.To = []string{"Nodes","Hubs"}    // sink set

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
			log.Fatalf("Open graph: %v", err)
			os.Exit(1)
		}

	} else {
		graph, err = db.CreateGraph(nil, gname, &options)

		if err != nil {
			log.Fatalf("Create graph: %v", err)
			os.Exit(1)
		}
	}

	// *** Nodes

	var frag_vertices A.Collection
	var node_vertices A.Collection
	var hub_vertices A.Collection

	frag_vertices, err = graph.VertexCollection(nil, "Fragments")

	if err != nil {
		log.Fatalf("Vertex collection Fragments: %v", err)
	}

	node_vertices, err = graph.VertexCollection(nil, "Nodes")

	if err != nil {
		log.Fatalf("Vertex collection Nodes: %v", err)
	}

	hub_vertices, err = graph.VertexCollection(nil, "Hubs")

	if err != nil {
		log.Fatalf("Vertex collection Hubs: %v", err)
	}

	// *** Links

	var F_edgeset A.Collection
	var C_edgeset A.Collection
	var E_edgeset A.Collection
	var N_edgeset A.Collection

	F_edgeset, _, err = graph.EdgeCollection(nil, "Follows")

	if err != nil {
		log.Fatalf("Egdes follows: %v", err)
	}

	C_edgeset, _, err = graph.EdgeCollection(nil, "Contains")

	if err != nil {
		log.Fatalf("Edges contains: %v", err)
	}

	E_edgeset, _, err = graph.EdgeCollection(nil, "Expresses")

	if err != nil {
		log.Fatalf("Edges expresses: %v", err)
	}

	N_edgeset, _, err = graph.EdgeCollection(nil, "Near")

	if err != nil {
		log.Fatalf("Edges near: %v", err)
	}

	g.S_db = db	
	g.S_graph = graph
	g.S_nodes = node_vertices
	g.S_hubs = hub_vertices
	g.S_frags = frag_vertices

	g.S_Follows = F_edgeset
	g.S_Contains = C_edgeset
	g.S_Expresses = E_edgeset	
	g.S_Near = N_edgeset

	return g
}

// **************************************************

func AddFrag(g Analytics, node Node) {

	exists,err := g.S_frags.DocumentExists(nil, node.Key)

	if !exists {
		_, err = g.S_frags.CreateDocument(nil, node)
		
		if err != nil {
			log.Fatalf("Failed to create non existent fragment: %s %v",node.Key,err)
			os.Exit(1);
		}

	}
}

// **************************************************

func AddNode(g Analytics, node Node) {

	//fmt.Println("Checking node",node)

	exists,err := g.S_nodes.DocumentExists(nil, node.Key)

	if !exists {
		_, err = g.S_nodes.CreateDocument(nil, node)
		
		if err != nil {
			log.Fatalf("Failed to create non existent node: %s %v",node.Key,err)
			os.Exit(1);
		}

	}
}

// **************************************************

func AddHub(g Analytics, node Node) {

	exists,err := g.S_hubs.DocumentExists(nil, node.Key)

	if !exists {
		_, err = g.S_hubs.CreateDocument(nil, node)
		
		if err != nil {
			log.Fatalf("Failed to create non existent node: %s %v",node.Key,err)
			os.Exit(1);
		}
	}

	//return dockey?
}

// **************************************************

func AddLink(g Analytics, link Link) {

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
		Weight: link.Weight,
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
			log.Fatalf("Failed to add new link: %v", err)
			os.Exit(1);
		}
	} else {

		// Don't need to check correct value, as each tuplet is unique, but check the weight
		
		var checkedge Link

		_,err := links.ReadDocument(nil,key,&checkedge)

		if err != nil {
			log.Fatalf("Failed to read value: %s %v",key,err)
			os.Exit(1);	
		}

		if checkedge != edge {

			fmt.Println("Correcting link weight",checkedge,"to",edge)

			_, err := links.UpdateDocument(nil, key, edge)

			if err != nil {
				log.Fatalf("Failed to update value: %s %v",edge,err)
				os.Exit(1);

			}
		}
	}
}

// **************************************************

func PrintNodes(ctx context.Context, db A.Database) {

	var err error
	var cursor A.Cursor

	querystring := "FOR doc IN Nodes LIMIT 1000 RETURN doc"

	cursor,err = db.Query(ctx,querystring,nil)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var doc Node

		_,err = cursor.ReadDocument(ctx,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("Doc returned: %v", err)
		} else {
			fmt.Print("Doc ",doc,"\n")
		}
	}
}

// **************************************************

func GetSuccessorsOf(g Analytics, node string, sttype int) SemanticLinkSet {

	return GetNeighboursOf(g,node,sttype,"+")
}

// **************************************************

func GetPredecessorsOf(g Analytics, node string, sttype int) SemanticLinkSet {

	return GetNeighboursOf(g,node,sttype,"-")
}

// **************************************************

func GetNeighboursOf(g Analytics, node string, sttype int, direction string) SemanticLinkSet {

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
		log.Fatalf("Neighbour query \"%s\"failed: %v", querystring,err)
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
			log.Fatalf("Doc returned: %v", err)
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

//**************************************************************

func GetPossibilityCone(g Analytics, start_key string, sttype int, visited map[string]bool) (Cone,int) {

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

func GetConePaths(g Analytics, start_key string, sttype int, visited map[string]bool) []string {

	// A cone is a sequence of spacelike slices orthogonal to the proper time defined by sttype
	// Each slice is formed from patches that spread from nodes in the current slice
	
	// width first

	var layer int = 0

	paths := GetPathsFrom(g, layer, start_key, sttype, visited)
	return paths
}

// **************************************************

func GetPathsFrom(g Analytics, layer int, startkey string, sttype int, visited map[string]bool) []string {

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

func MakeAssociations(collname string, db A.Database, kv map[string]Association) {

	// Create collection

	var err error
	var coll_exists bool
	var coll A.Collection

	coll_exists, err = db.CollectionExists(nil, collname)

	if coll_exists {
		fmt.Println("Collection " + collname +" exists already")

		coll, err = db.Collection(nil, collname)

		if err != nil {
			log.Fatalf("Existing collection: %v", err)
			os.Exit(1)
		}

	} else {

		coll, err = db.CreateCollection(nil, collname, nil)

		if err != nil {
			log.Fatalf("Failed to create collection: %v", err)
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
		log.Fatalf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var assoc Association

		_,err = cursor.ReadDocument(nil,&assoc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			log.Fatalf("Assoc returned: %v", err)
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
			log.Fatalf("Failed to create non existent node: %s %v",key,err)
			os.Exit(1);
		}
	} else {

		var checkassoc Association
		
		_,err = coll.ReadDocument(nil,key,&checkassoc)

		if checkassoc != assoc {
			fmt.Println("Correcting data",checkassoc,"to",assoc)
			_, err := coll.UpdateDocument(nil, key, assoc)
			if err != nil {
				log.Fatalf("Failed to update value: %s %v",assoc,err)
				os.Exit(1);

			}
		}
	}
}



// ****************************************************************************
// Set/Collection Aggregation
// ****************************************************************************

type Set map[string][]string

// ****************************************************************************

func TogetherWith(sets Set, a1,a2 string) {

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
			for m := range sets[s1] {
				sets[s2] = append(sets[s2],sets[s1][m])
			}
			delete(sets,s1)
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

func BelongsToSet(sets Set,member string) (bool,string,string) {

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

// ****************************************************************************
// * Document Scanner
// ****************************************************************************

func LoadContext() {

	STM_CONTEXT_RANK = make(map[string]float64)

	name := fmt.Sprintf("/tmp/cellibrium/context")

	f, err := os.Open(name)

	if err != nil {
		fmt.Println("Error opening file ",name)
		return
	}

	var key string
	var value float64

	br := bufio.NewReader(f)

	for {
		ln, _, err := br.ReadLine()
		
		if err != nil {
			break
		}
		
		array := strings.Split(string(ln),",")
		
		key = strings.Trim(array[0],"\n ")
		value, err = strconv.ParseFloat(array[1], 64)

		STM_CONTEXT_RANK[key] = value
	}

	f.Close()
}

//**************************************************************

func SaveContext() {

	name := fmt.Sprintf("/tmp/cellibrium/context")

	f, err := os.Create(name)
	
	if err != nil {
		fmt.Println("Error opening file ",name)
		return
	}

	// Key Value store

	for key := range STM_CONTEXT_RANK {
		
		s := fmt.Sprintf("%s,%f\n",key,STM_CONTEXT_RANK[key])
		f.WriteString(s)
	}
	
	f.Close()
}

//**************************************************************

func TickContext(search_term string) {

	var rrbuffer [MAXCLUSTERS][]string

	no_dot := strings.ReplaceAll(search_term,"."," ")
	no_comma := strings.ReplaceAll(no_dot,","," ")

	cleaned := strings.Split(no_comma," ")

	for word := range cleaned {

		for i := 2; i < MAXCLUSTERS; i++ {
			
			// Pop from round-robin
			
			if (len(rrbuffer[i]) > i-1) {
				rrbuffer[i] = rrbuffer[i][1:i]
			}
			
			// Push new to maintain length
			
			rrbuffer[i] = append(rrbuffer[i],cleaned[word])
			
			// Assemble the key, only if complete cluster
			
			if (len(rrbuffer[i]) > i-1) {
				
				var key string
				
				for j := 0; j < i; j++ {
					key = key + rrbuffer[i][j]
					if j < i-1 {
						key = key + " "
					}
				}
				
				// Add here - listener context flag certain terms of interest (danger signals)
				
				if BrokenPromise(rrbuffer[i][0],rrbuffer[i][i-1]) {
					continue
				}
				
				TickUpdateContext(key)
			}
		}
	
		TickUpdateContext(cleaned[word])
	}
}

//**************************************************************

func TickUpdateContext(key string) {

	var rank float64
	const REPEATED_HERE_AND_NOW  = 1.0

	if Irrelevant(key) {
		return
	}

	if _, ok := STM_CONTEXT_RANK[key]; !ok {

		rank = INITIAL_VALUE

	} else {

		rank = REPEATED_HERE_AND_NOW
	}

	STM_CONTEXT_RANK[key] = rank

	MemoryDecay()

}

// ****************************************************************************

func StringContext(search_term string) []string {

	var context []string
	var rrbuffer [MAXCLUSTERS][]string

	no_dot := strings.ReplaceAll(search_term,"."," ")
	no_comma := strings.ReplaceAll(no_dot,","," ")

	cleaned := strings.Split(no_comma," ")

	for word := range cleaned {

		for i := 2; i < MAXCLUSTERS; i++ {
			
			// Pop from round-robin
			
			if (len(rrbuffer[i]) > i-1) {
				rrbuffer[i] = rrbuffer[i][1:i]
			}
			
			// Push new to maintain length
			
			rrbuffer[i] = append(rrbuffer[i],cleaned[word])
			
			// Assemble the key, only if complete cluster
			
			if (len(rrbuffer[i]) > i-1) {
				
				var key string
				
				for j := 0; j < i; j++ {
					key = key + rrbuffer[i][j]
					if j < i-1 {
						key = key + " "
					}
				}
				
				// Add here - listener context flag certain terms of interest (danger signals)
				
				if BrokenPromise(rrbuffer[i][0],rrbuffer[i][i-1]) {
					continue
				}
				
				context = append(context,key)
			}
		}
	
		context = append(context,cleaned[word])
	}

	return context
}

//**************************************************************

func MemoryDecay() {

	const decay_rate = 0.00001
	const context_threshold = INITIAL_VALUE

	for k := range STM_CONTEXT_RANK {

		oldv := STM_CONTEXT_RANK[k]
		
		// Can't go negative
		
		if oldv > decay_rate {
			
			STM_CONTEXT_RANK[k] = oldv - decay_rate

		} else {
			// Help prevent memory blowing up - garbage collection

			//fmt.Println("DELETING",k)
			delete(STM_CONTEXT_RANK,k)
		}
	}
}

//**************************************************************

func BrokenPromise(firstword,lastword string) bool {

	// A standalone fragment can't start/end with these words, because they
	// Promise to bind to something else...
	// Rather than looking for semantics, look at spacetime promises only - words that bind strongly
	// to a prior or posterior word.

	if (len(firstword) == 1) || len(lastword) == 1 {
		return true
	}

	var eforbidden = []string{"but", "and", "the", "or", "a", "an", "its", "it's", "their", "your", "my", "of" }

	for s := range eforbidden {
		if lastword == eforbidden[s] {
			return true
		}
	}

	var sforbidden = []string{"and","or","of"}

	for s := range sforbidden {
		if firstword == sforbidden[s] {
			return true
		}
	}

return false 
}
//**************************************************************

func CompareContexts(list1,list2 []string) (float64,[]string) {

	// find the aligned overlap of the lists as if vectors

	var overlap_unique = make(map[string]bool)

	if len(list1) == 0 || len(list2) == 0 {

		var nothing []string
		return 0,nothing
	}

	var total1,total2 int = 0,0

	// Get total weights for normalization

	for s1 := range list1 {
		if strings.Contains(list1[s1],":") {
			prefix := strings.Split(list1[s1],":")
			val, _ := strconv.Atoi(prefix[0])
			total1 += val

		} else {
			total1 += 1
		}
	}

	for s2 := range list2 {
		if strings.Contains(list2[s2],":") {
			prefix := strings.Split(list2[s2],":")
			val, _ := strconv.Atoi(prefix[0])
			total2 += val

		} else {
			total2 += 1
		}
	}

	for s1 := range list1 {

		for s2 := range list2 {

			if list1[s1] == list2[s2] {

				if Irrelevant(list1[s1]) {
					continue
				}

				// Could try to compare without endings s,ed,ing on verbs later

				if len(list1[s1]) > 0 {
					overlap_unique[list1[s1]] = true
				}

				list1[s1] = ""
				list2[s2] = ""

				break
			}
		}
	}

	var overlap []string
	var sum_weights int = 0

	for s := range overlap_unique {

		if strings.Contains(s,":") {
			prefix := strings.Split(s,":")
			val, _ := strconv.Atoi(prefix[0])

			// could exclude val > 3 to exclude long fragments from the match
			if val > 3 {
				continue // makes no difference as the likelihood is tiny
			}

			sum_weights += val

		} else {
			sum_weights += 1
		}

		overlap = append(overlap,s)
	}

	const average_sentence_in_words = 30.0

	overlap_weights := 100 * float64(sum_weights) / average_sentence_in_words // not float64(total1+total2)
	
	//fmt.Println("OVERLAP ",overlap)

	// We should really compare this to a sentence / event length, not to the size of the data
	//of the length of the fragments! An average sentence length is

	return overlap_weights, overlap
}

//**************************************************************

func Irrelevant(word string) bool {

	if len(word) < 3 {
		return true
	}

	var irrel = []string{"hub:", "but", "and", "the", "or", "a", "an", "its", "it's", "their", "your", "my", "of", "if", "we", "you", "i", "there", "as", "in", "then", "that", "with", "to", "is","was", "when", "where", "are", "some", "can", "also", "it", "at", "out", "like", "they", "her", "him", "them", "his", "our", "by", "more", "less", "from", "over", "under", "why", "because", "what", "every", "some", "about", "though", "for", "around", "about", "any", "will","had","all","which" }

	for s := range irrel {
		if irrel[s] == word {
			return true
		}
	}

return false
}

