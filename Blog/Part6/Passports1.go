
package main

import (
	"fmt"
	"strings"
	S "SST"
)

// ********************************************************************************
// * Passport v1: Journeys as sequential processes, event sequences
// ********************************************************************************

func main() {

	var dbname string = "NationSpacetime"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	var g S.Analytics

	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	/* Modelling choices ------------

        Let's use Nodes are Events
                  Frags for Persons
                  Hubs  for Locations to use the unmodified SST code

        We make every transaction an event in the process timeline of global travel

        Visas are a process between country and person, a `right'
        expressed and granted by a country

        ---------------------------------- */

	// Mark's journey as a sequential process

	CountryIssuedPassport(&g,"Professor Burgess","UK","Number 12345")
	CountryIssuedVisa(&g,"Professor Burgess","USA","Visa Waiver")
	PersonLocation(&g,"Professor Burgess","USA")
	PersonLocation(&g,"Professor Burgess","UK")

	// This could be a problem, because we haven't made a collection for cities
	// Requires some additional logic

	paris := S.CreateHub(g,"Paris","Paris, capital city in France",1)
	france := S.CreateHub(g,"France","France, country in Europe",100)

	CountryIssuedVisa(&g,"Emily","France","Schengen work visa")
	PersonLocation(&g,"Emily","Paris")

	S.CreateLink(g,paris,"PART_OF",france,100)

	// Captain Evil's journey as a sequential process

	CountryIssuedVisa(&g,"Captain Evil","USA","Work Visa")
	PersonLocation(&g,"Captain Evil","UK")
	PersonLocation(&g,"Captain Evil","USA")

}

//****************************************************

func PersonLocation(g *S.Analytics, person, location string) {

	// First to associate a person with a location as an event, we form an event hub
        // we don't have details here, so just add empty values or don't channge

	// Darmok

	person_id := strings.ReplaceAll(person," ","_")

	S.CreateFragment(*g, person_id, person, 0)

	// Tanagra

	S.CreateHub(*g, location, "", 0)

	// Event: Darmok, Gillard at Tanagra

	var short,long string
	
	short = strings.ReplaceAll(person + " in " + location," ","_")
	long = person + " observed in " + location

	// Add to proper timeline

	fmt.Println("Timeline: ",short)
	S.NextDataEvent(g,short,long)

}

//****************************************************

func CountryIssuedPassport(g *S.Analytics, person, location, passport string) {

	country_hub := S.CreateHub(*g, location, "", 0)

	person_id := strings.ReplaceAll(person," ","_")
	person_node := S.CreateFragment(*g, person_id, "", 0)

	time_limit := 1.0

	pass_id := strings.ReplaceAll(passport," ","_")

	S.ASSOCIATIONS[pass_id] = S.Association{pass_id,S.GR_EXPRESSES,"grants passport to","holds passport from","did not grant passport to","does not hold passport from"}

	S.CreateLink(*g, country_hub, pass_id, person_node, time_limit)

	// Now the event

	var short,long string
	
	short = strings.ReplaceAll(location + " grants " + passport + " to " + person," ","_")
	long = location + " granted passport " + passport + " to " + person

	// Add to proper timeline

	fmt.Println("Timeline: ",long)
	S.NextDataEvent(g,short,long)

}

//****************************************************

func CountryIssuedVisa(g *S.Analytics, person, location, visa string) {

	country_hub := S.CreateHub(*g, location, "", 0)

	person_id := strings.ReplaceAll(person," ","_")
	person_node := S.CreateFragment(*g, person_id, "", 0)

	time_limit := 1.0

	visa_id := strings.ReplaceAll(visa," ","_")

	S.ASSOCIATIONS[visa_id] = S.Association{visa_id,S.GR_EXPRESSES,"grants visa to","holds visa from","does not visa to","does not hold visa from"}

	S.CreateLink(*g, country_hub, visa_id, person_node, time_limit)

	// Now the event

	var short,long string
	
	short = strings.ReplaceAll(location + " grants " + visa + " to " + person," ","_")
	long = location + " grants visa " + visa + " to " + person

	// Add to proper timeline

	fmt.Println("Timeline: ",long)
	S.NextDataEvent(g,short,long)

}
