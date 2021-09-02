
package main

import (
	"os"
	"fmt"
	"hash/fnv"
	"strings"
	A "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"

)

// ********************************************************************************
// * Passport v2: modifying the SST library by pasting in directly (see below)
// ********************************************************************************

type Model struct {

// Container db

S_db   A.Database

// Graph model

S_graph A.Graph

// Node types

S_locations A.Collection
S_countries A.Collection
S_persons   A.Collection
S_events    A.Collection

// 4 primary link types

S_Follows   A.Collection
S_Contains  A.Collection
S_Expresses A.Collection
S_Near      A.Collection

// Chain memory 
previous_event_key Node
}

// ****************************************************************************

// Add some fields here for use-case

type Node struct {
	Key          string `json:"_key"`
	Description  string `json: "description"`
	Number       int
	Prefix       string
	Weight       float64 `json:"weight"`   // importance rank
}

// ***************************************************************************

type Link struct {
	From     string `json:"_from"`
	To       string `json:"_to"`  
        SId      string `json:"semantics"`
	Number      int `json:"number"`
	Weight  float64 `json:"weight"`
	Key      string `json:"_key"`
}

// ****************************************************************************

type Association struct {

	Key     string    `json:"_key"`

	STtype  int       `json:"STType"`
	Fwd     string    `json:"Fwd"`
	Bwd     string    `json:"Bwd"` 
	NFwd    string    `json:"NFwd"`
	NBwd    string    `json:"NBwd"`
}

// **********************************************************************

var ASSOCIATIONS = make(map[string]Association)

const GR_NEAR int      = 1  // approx like
const GR_FOLLOWS int   = 2  // i.e. influenced by
const GR_CONTAINS int  = 3 
const GR_EXPRESSES int = 4  // represents, etc

// ***********************************************************************

func main() {

	var dbname string = "ModifiedNationSpacetime"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g Model

	g = OpenModel(dbname, service_url, user, pwd)

	CreatePerson(g,"markburgess_osl", "Professor Mark Burgess",123456,0)

	CreateCountry(g,"USA","United States of America")
	CreateCountry(g,"UK","United Kingdom")

	france := CreateCountry(g,"France","France, country in Europe")
	paris := CreateLocation(g,"Paris","Paris, capital city in France")
	CreateLink(g,paris,"PART_OF",france,0,0)

	// Mark's journey as a sequential process

	CountryIssuedPassport(&g,"Professor Burgess","UK","Number 12345")
	CountryIssuedVisa(&g,"Professor Burgess","USA","Visa Waiver")
	PersonLocation(&g,"Professor Burgess","USA")
	PersonLocation(&g,"Professor Burgess","UK")

	// This could be a problem, because we haven't made a collection for cities
	// Requires some additional logic

	CountryIssuedVisa(&g,"Emily","France","Schengen work visa")
	PersonLocation(&g,"Emily","Paris")

	// Captain Evil's journey as a sequential process

	CountryIssuedVisa(&g,"Captain Evil","USA","Work Visa")
	PersonLocation(&g,"Captain Evil","UK")
	PersonLocation(&g,"Captain Evil","USA")

}

//****************************************************

func PersonLocation(g *Model, person, location string) {

	// First to associate a person with a location as an event, we form an event hub
        // we don't have details here, so just add empty values or don't channge

	// Darmok

	person_id := strings.ReplaceAll(person," ","_")

	CreatePerson(*g, person_id, "", 0, 0)

	// Tanagra

	CreateLocation(*g, location, "")

	// Event: Darmok, Gillard at Tanagra

	var short,long string
	
	short = strings.ReplaceAll(person + " in " + location," ","_")
	long = person + " observed in " + location

	// Add to proper timeline

	fmt.Println("Timeline: ",short)
	NextDataEvent(g,short,long)

}

//****************************************************

func CountryIssuedPassport(g *Model, person, location, passport string) {

	country_hub := CreateLocation(*g, location, "")

	person_id := strings.ReplaceAll(person," ","_")
	person_node := CreatePerson(*g, person_id, "", 0, 0)

	time_limit := 1

	pass_id := strings.ReplaceAll(passport," ","_")

	ASSOCIATIONS[pass_id] = Association{pass_id,GR_EXPRESSES,"grants passport to","holds passport from","did not grant passport to","does not hold passport from"}

	CreateLink(*g, country_hub, pass_id, person_node, time_limit, 0)

	// Now the event

	var short,long string
	
	short = strings.ReplaceAll(location + " grants " + passport + " to " + person," ","_")
	long = location + " granted passport " + passport + " to " + person

	// Add to proper timeline

	fmt.Println("Timeline: ",long)
	NextDataEvent(g,short,long)

}

//****************************************************

func CountryIssuedVisa(g *Model, person, location, visa string) {

	country_hub := CreateLocation(*g, location, "")

	person_id := strings.ReplaceAll(person," ","_")
	person_node := CreatePerson(*g, person_id, "", 0, 0)

	time_limit := 1

	visa_id := strings.ReplaceAll(visa," ","_")

	ASSOCIATIONS[visa_id] = Association{visa_id,GR_EXPRESSES,"grants visa to","holds visa from","does not visa to","does not hold visa from"}

	CreateLink(*g, country_hub, visa_id, person_node, time_limit, 0)

	// Now the event

	var short,long string
	
	short = strings.ReplaceAll(location + " grants " + visa + " to " + person," ","_")
	long = location + " grants visa " + visa + " to " + person

	// Add to proper timeline

	fmt.Println("Timeline: ",long)
	NextDataEvent(g,short,long)

}

//**************************************************************
// SST Modified
//**************************************************************

func InitializeSmartSpaceTime() {

	ASSOCIATIONS["CONTAINS"] = Association{"CONTAINS",GR_CONTAINS,"contains","belongs to or is part of","does not contain","is not part of"}

	ASSOCIATIONS["GENERALIZES"] = Association{"GENERALIZES",GR_CONTAINS,"generalizes","is a special case of","is not a generalization of","is not a special case of"}

	// reversed case of containment semantics

	ASSOCIATIONS["PART_OF"] = Association{"PART_OF",-GR_CONTAINS,"incorporates","is part of","is not part of","doesn't contribute to"}

	// *

	ASSOCIATIONS["ORIGINATES_FROM"] = Association{"ORIGINATES_FROM",GR_FOLLOWS,"originates from","is the source/origin of","does not originate from","is not the source/origin of"}

	// *

	ASSOCIATIONS["CAUSEDBY"] = Association{"CAUSEDBY",GR_FOLLOWS,"caused by","may cause","was not caused by","probably didn't cause"}

	ASSOCIATIONS["DERIVES_FROM"] = Association{"DERIVES_FROM",GR_FOLLOWS,"derives from","leads to","does not derive from","does not leadto"}

	ASSOCIATIONS["DEPENDS"] = Association{"DEPENDS",GR_FOLLOWS,"may depend on","may determine","doesn't depend on","doesn't determine"}

	// Neg

	ASSOCIATIONS["THEN"] = Association{"THEN",-GR_FOLLOWS,"then","previously","but not","didn't follow"}

	ASSOCIATIONS["LEADS_TO"] = Association{"LEADS_TO",-GR_FOLLOWS,"leads to","doesn't imply","doen't reach","doesn't precede"}

	ASSOCIATIONS["PRECEDES"] = Association{"PRECEDES",-GR_FOLLOWS,"precedes","follows","doen't precede","doesn't precede"}

	// *

	ASSOCIATIONS["ALIAS"] = Association{"ALIAS",GR_NEAR,"also known as","also known as","not known as","not known as"}

	ASSOCIATIONS["IS_LIKE"] = Association{"IS_LIKE",GR_NEAR,"is similar to","is similar to","is unlike","is unlike"}

}

//***********************************************************************

func OpenModel(dbname, service_url, user, pwd string) Model {

	var g Model
	var db A.Database

	InitializeSmartSpaceTime()

	db = OpenDatabase(dbname, service_url, user, pwd)

	// Book-keeping: wiring up edgeCollection to store the edges

	var F_edges A.EdgeDefinition
	var C_edges A.EdgeDefinition
	var N_edges A.EdgeDefinition
	var E_edges A.EdgeDefinition

	F_edges.Collection = "Follows"
	F_edges.From = []string{"Events","Persons","Locations","Countries"}  // source set
	F_edges.To = []string{"Events","Persons","Locations","Countries"}    // sink set

	C_edges.Collection = "Contains"
	C_edges.From = []string{"Events","Persons","Locations","Countries"}              // source set
	C_edges.To = []string{"Events","Persons","Locations","Countries"}    // sink set

	N_edges.Collection = "Near"
	N_edges.From = []string{"Events","Persons","Locations","Countries"}  // source set
	N_edges.To = []string{"Events","Persons","Locations","Countries"}    // sink set

	E_edges.Collection = "Expresses"
	E_edges.From = []string{"Events","Persons","Locations","Countries"}  // source set
	E_edges.To = []string{"Events","Persons","Locations","Countries"}    // sink set

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

	var person_vertices A.Collection
	var event_vertices A.Collection
	var country_vertices A.Collection
	var location_vertices A.Collection

	person_vertices, err = graph.VertexCollection(nil, "Persons")

	if err != nil {
		fmt.Printf("Vertex collection Persons: %v", err)
	}

	event_vertices, err = graph.VertexCollection(nil, "Events")

	if err != nil {
		fmt.Printf("Vertex collection Nodes: %v", err)
	}

	country_vertices, err = graph.VertexCollection(nil, "Countries")

	if err != nil {
		fmt.Printf("Vertex collection Hubs: %v", err)
	}

	location_vertices, err = graph.VertexCollection(nil, "Locations")

	if err != nil {
		fmt.Printf("Vertex collection Hubs: %v", err)
	}

	// *** Links - 4 ST types

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
	g.S_countries = country_vertices
	g.S_locations = location_vertices
	g.S_persons = person_vertices
	g.S_events = event_vertices

	g.S_Follows = F_edgeset
	g.S_Contains = C_edgeset
	g.S_Expresses = E_edgeset	
	g.S_Near = N_edgeset

	g.previous_event_key = Node{ Key: "start" }

	return g
}

//***********************************************************************

func OpenDatabase(name, url, user, pwd string) A.Database {

	var db A.Database
	var db_exists bool
	var err error
	var client A.Client

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

	db_exists, err = client.DatabaseExists(nil,name)

	if db_exists {

		db, err = client.Database(nil,name)

	} else {
		db, err = client.CreateDatabase(nil,name, nil)
		
		if err != nil {
			fmt.Printf("Failed to create database: %v", err)
			os.Exit(1);
		}
	}

	return db
}

// ****************************************************************************
//  Graph invariants
// ****************************************************************************

func CreateLink(g Model, c1 Node, rel string, c2 Node, number int, weight float64) {

	var link Link

	//fmt.Println("CreateLink: c1",c1,"rel",rel,"c2",c2)

	link.From = c1.Prefix + strings.ReplaceAll(c1.Key," ","_")
	link.To = c2.Prefix + strings.ReplaceAll(c2.Key," ","_")
	link.SId = ASSOCIATIONS[rel].Key
	link.Weight = weight
	link.Number = number

	if link.SId != rel {
		fmt.Println("Associations not set up -- missing InitializeSmartSpacecTime?")
		os.Exit(1)
	}

	AddLink(g,link)
}

// ****************************************************************************

func CreatePerson(g Model, short_description,vardescription string, number int, weight float64) Node {

	var concept Node

	description := InvariantDescription(vardescription)

	concept.Description = description
	concept.Number = number
	concept.Key = short_description
	concept.Prefix = "Persons/"
	concept.Weight = weight

	AddPerson(g,concept)

	return concept
}

// ****************************************************************************

func CreateCountry(g Model, short_description,vardescription string) Node {

	var concept Node

	description := InvariantDescription(vardescription)

	concept.Description = description
	concept.Key = short_description
	concept.Prefix = "Countries/"

	AddCountry(g,concept)

	return concept
}

// ****************************************************************************

func CreateLocation(g Model, short_description,vardescription string) Node {

	var concept Node

	description := InvariantDescription(vardescription)

	concept.Description = description
	concept.Key = short_description
	concept.Prefix = "Locations/"

	AddLocation(g,concept)

	return concept
}

// ****************************************************************************

func CreateEvent(g Model, short_description,vardescription string) Node {

	var concept Node

	description := InvariantDescription(vardescription)

	concept.Description = description
	concept.Key = short_description
	concept.Prefix = "Events/"

	AddEvent(g,concept)

	return concept
}


// ****************************************************************************

func AddEvent(g Model, node Node) {

	var coll A.Collection = g.S_events
	InsertNodeIntoCollection(g,node,coll)
}

// ****************************************************************************

func AddPerson(g Model, node Node) {

	var coll A.Collection = g.S_persons
	InsertNodeIntoCollection(g,node,coll)
}

// ****************************************************************************

func AddCountry(g Model, node Node) {

	var coll A.Collection = g.S_countries
	InsertNodeIntoCollection(g,node,coll)
}

// ****************************************************************************

func AddLocation(g Model, node Node) {

	var coll A.Collection = g.S_locations
	InsertNodeIntoCollection(g,node,coll)
}

// **************************************************

func InsertNodeIntoCollection(g Model, node Node, coll A.Collection) {
	
	exists,err := coll.DocumentExists(nil, node.Key)
	
	if !exists {
		_, err = coll.CreateDocument(nil, node)
		
		if err != nil {
			fmt.Println("Failed to create non existent node in InsertNodeIntoCollection: ",node,err)
			os.Exit(1);
		}

	} else {

		// Don't need to check correct value, as each tuplet is unique, but check the data

		if node.Description == "" && node.Weight == 0  && node.Number == 0 {
			// Leave the values alone if we don't mean to update them
			return
		}
		
		var checknode Node

		_,err := coll.ReadDocument(nil,node.Key,&checknode)

		if err != nil {
			fmt.Printf("Failed to read value: %s %v",node.Key,err)
			os.Exit(1);	
		}

		if checknode != node {

			//fmt.Println("Correcting link values",checknode,"to",node)

			_, err := coll.UpdateDocument(nil, node.Key, node)

			if err != nil {
				fmt.Printf("Failed to update value: %s %v",node,err)
				os.Exit(1);

			}
		}
	}
}

// **************************************************

func AddLink(g Model, link Link) {

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
			fmt.Println("Failed to add new link", err, link, edge)
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

			fmt.Println("Correcting link weight",checkedge,"to",edge)

			_, err := links.UpdateDocument(nil, key, edge)

			if err != nil {
				fmt.Printf("Failed to update value: %s %v",edge,err)
				os.Exit(1);

			}
		}
	}
}

// ****************************************************************************

func NextDataEvent(g *Model, shortkey, data string) Node {

	key  := CreateEvent(*g, shortkey, data)
	
	if g.previous_event_key.Key != "start" {
		
		CreateLink(*g, g.previous_event_key, "THEN", key, 1.0, 0)
	}
	
	g.previous_event_key = key

	return key 
}

//**************************************************************

func InvariantDescription(s string) string {

	return strings.Trim(s,"\n ")
}

// **************************************************************

func fnvhash(b []byte) string {
        hash := fnv.New64a()
        hash.Write(b)
        h := hash.Sum64()
        return fmt.Sprintf("key_%d",h)
}



