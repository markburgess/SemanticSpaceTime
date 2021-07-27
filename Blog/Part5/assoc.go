
//
// Coding graphs or associations in Go
//

package main

import (
	"fmt"
)

// ************************************************************

func main() {

	var child_of = make(map[string]string)
	
	child_of["A"] = "B"

	fmt.Println(child_of)

	// Adjacency matrix approach

	type VectorPair struct {
		From string
		To string
	}

	var employs = make(map[VectorPair]bool)

	employs[VectorPair{From: "A", To: "B"}] = true

	fmt.Println(employs)

}
