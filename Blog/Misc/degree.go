
package main

import (
	"fmt"
	"sort"

	S "SST"
	A "github.com/arangodb/go-driver"

)

// ********************************************************************************

type Result struct {

	K  string `json:"K"`
	V  int    `json:"V"`
}

// ********************************************************************************

func main() {
	
	var dbname string = "TestSST"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g S.Analytics
	
	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	// Get node degree distributions

	var k_in = make(map[string]int,0)
	var k_out = make(map[string]int,0)
	var k = make(map[string]int,0)

	GetDegreeDistribution(g,k_in,k_out,k)

	//var keys map[string]int
	var degrees []int = make([]int,0) 

		fmt.Printf("\n%20s %s %s %s\n","Node", "k_in", "k_out", "k")

	for name := range k {
		degrees = append(degrees,k[name])
		fmt.Printf("%20s %3d %3d %3d\n",name, k_in[name], k_out[name], k[name])
	}

	fmt.Println("\nSorted histogram:")

	sort.Ints(degrees)

	for i := len(degrees)-1; i >= 0; i-- {
		fmt.Println(degrees[i])
	}

	var sum int = 0
	var k_av float64 = 0

	for  name := range k_out {
		sum += k_out[name]
	}

	k_av = float64(sum) / float64(len(k_out))

	fmt.Printf("Effective dimension as average k_out = %f\n",k_av)
}

// ********************************************************************************

func GetDegreeDistribution(g S.Analytics, k_in,k_out,k map[string]int) {

	var err error
	var cursor A.Cursor

	instring := "FOR n in Follows COLLECT node = n._to INTO inn RETURN { K: node, V: COUNT(inn[*])}"
	outstring := "FOR n in Follows COLLECT node = n._from INTO out RETURN { K: node, V: COUNT(out[*])}"

	cursor,err = g.S_db.Query(nil,instring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var doc Result

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			k_in[doc.K] = doc.V
		}
	}

	cursor,err = g.S_db.Query(nil,outstring,nil)

	if err != nil {
		fmt.Printf("Query failed: %v", err)
	}

	defer cursor.Close()

	for {
		var doc Result

		_,err = cursor.ReadDocument(nil,&doc)

		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			k_out[doc.K] = doc.V
			k[doc.K] = k_out[doc.K] + k_in[doc.K]
		}
	}
}

