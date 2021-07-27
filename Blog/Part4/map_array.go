

package main

import ("fmt")

// ********************************************************************************

type NodePair struct {

	// We want this datatype in order to use as a map key
	// for quick lookup to speed up node searching - overhead or gain?

	From string
	To   string  // next (fwd) neighbour-node
}

func main() {

	var x NodePair

	x.From = "xxx"
	x.To = "xxxx"

	var fish = make(map[NodePair]string)

	fish[x] = "cod"

	fmt.Println(fish[x])
}