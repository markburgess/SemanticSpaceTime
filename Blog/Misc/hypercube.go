
package main

import (
	"fmt"
	S "SST"
	//A "github.com/arangodb/go-driver"

)

// ********************************************************************************

const N = 3
const L = 100

// ********************************************************************************

func main() {
	
	var dbname string = fmt.Sprintf("hypercube_%d",N)
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g S.Analytics
	
	g = S.OpenAnalytics(dbname, service_url, user, pwd)

	// Build a hypercube in N dimensions

	var basis_clock [N]int

	for {
		Increment(&basis_clock,0)
		
		ConnectKeyNode(g, basis_clock)

		if Max(basis_clock) {
			break
		}
	}
}

/*******************************************************************/

func ConnectKeyNode(g S.Analytics, position [N]int) {

	key := GetKey(position)

	this := S.CreateNode(g,key,"",0)

	// Connect lattice to predecessor in each dimension

	for dim := 0; dim < N; dim++ {

		if position[dim] > 0 {

			position[dim]--
			prev := GetKey(position)
			position[dim]++

			pred := S.CreateNode(g,prev,"",0)

			S.CreateLink(g,this,"CONNECTED",pred,0)
		}
	}
}

/*******************************************************************/

func GetKey(position [N]int) string {

	var key string = ""

	for dim := 0; dim < N; dim++ {

		key = key + fmt.Sprintf(",%d",position[dim])
	}

return key
}

/*******************************************************************/

func Increment(basis *[N]int, dim int) {

	if basis[dim] < L {
		
		basis[dim]++
		
	} else {
		basis[dim] = 0
		
		if dim < N {
			Increment(basis,dim+1)
		}
	}
}

/*******************************************************************/

func Max(clock [N]int) bool {

	for dim := 0; dim < N; dim++ {
		if clock[dim] != L {
			return false
		}
	}

return true
}