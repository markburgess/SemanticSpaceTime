
package main

import (
	"fmt"
	"os"
	"time"
	E "Entangle"
)

const max_rounds = 1000000

// ****************************************************************************
// * Entanglement 3

// In this version, we change....
// ****************************************************************************

func main() {

	// go run entangle L or R

	id := os.Args[1]

	var tick int = 0

	Init()

	for resonate := 0.0; resonate < max_rounds; resonate++ {

		d := E.DetectorMinus(id)

		// Anti-correlate q=qz, stop once each channel end has attched to detector
		
		qBARq(id, d, tick)
		
		tick++

		time.Sleep(E.TICK) // read!
	}
}

//***********************************************************

func qBARq(id string, d []byte, tick int) {

	var q = make([]byte,E.WAVELENGTH)

	// This file is the image of the (+) promise FROM the other end TO this agent

	qbar := E.QMinus(id)

	// Faults and noise should be rare for state persistence

	if E.Undefined(qbar) {		
		return
	}

	q = E.Bar(qbar)

	// The detector is a process that reads the phase of the incoming process

	fmt.Println("|",id,"> =",string(q),"detector", string(d))
 
	E.QPlus(id,q)

}

//***********************************************************

func Init() {

	os.Remove("/tmp/paired")
}
