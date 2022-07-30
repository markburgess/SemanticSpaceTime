
package main

import (
	"fmt"
	"time"
	"math"
	E "Entangle"
)

const max_rounds = 500
const showspins = false

// ****************************************************************************
// * OBSERVATION v3 - start this before the entangled pair
// * in this version the qbarq process senses the detector boundary conditions
// * 
// ****************************************************************************

func main () {

	const thetaL float64 = 90.0
	const thetaR float64 = 90.0

	for theta_L := -thetaL; theta_L <= thetaL; theta_L += 90 {
		for theta_R := -thetaR; theta_R <= thetaR; theta_R += 45 {

			E,Delta := Trial(theta_L,theta_R)

			fmt.Printf("(%3.1f,%6.1f) %.3f %.3f (%.3f)\n",theta_L,theta_R,E,Delta,-math.Cos((theta_L-theta_R)/360.0*2*3.14))
		}
	}
}

// ****************************************************************************

func Trial(thetaL,thetaR float64) (float64,float64) {

	// Sampling affinity for q and qbar

	var P,Delta,pyy,pnn,pyn,pny float64
	var N,Nyy,Nnn,Nyn,Nny int = 0,0,0,0,0

	for rounds := 0; rounds < max_rounds; rounds++ {

		E.NewPair()

		// wait for equilibrium (don't set noise rate too high or distance too short)

		time.Sleep(E.EQUILIBRATE)

		// These can't be simultaneous but within the stability window

		E.SetDetector("L",thetaL)
		E.SetDetector("R",thetaR)

		eL := Observe("L")
		eR := Observe("R")

		if showspins {
			if (eL != 0 && eR != 0) {
				fmt.Printf("expt %4d:      L = %4d,     R = %4d\n",rounds,eL,eR)
			} else {
				fmt.Printf("expt %4d:      L = %4d,     R = %4d  (no result)\n",rounds,eL,eR)
			}
		}
		
		if eL == -1 && eR == 1 {
			Nny++
		}
		
		if eL == 1 && eR == -1 {
			Nyn++
		}
		
		if eL == 1 && eR == 1 {
			Nyy++
		}
		
		if eL == -1 && eR == -1 {
			Nnn++
		}
		
		N++
		
		// sum up the values over multiple trials to get stats
		// sampling should be slower than the entanglement process

		NTOT := float64(Nyy+Nnn+Nyn+Nny)

		P = float64(Nyy+Nnn-Nyn-Nny)/NTOT

		pyy = float64(Nyy)/NTOT
		pnn = float64(Nnn)/NTOT
		pyn = float64(Nyn)/NTOT
		pny = float64(Nny)/NTOT

		if N % 100 == 0 {
			//fmt.Printf("%8d P(%.0f,%.0f) =  %.3f (yy=%.3f,nn=%.3f,yn=%.3f,ny=%.3f) %.1d=%d\n",rounds,thetaL,thetaR,P,pyy,pnn,pyn,pny,NTOT,N)
		}
	}

	// Error estimate
	Delta = math.Sqrt((pyy-pnn)*(pyy-pnn) + (pny-pyn)*(pny-pyn))

	return P,Delta
}

//***********************************************************

func Observe(id string) int {

	const retry = 200
	var q,d []byte

	for sample := 0; sample < retry; sample++ {

		if len(q) == 0 {
			q = E.QMinus(id)
		}

		if len(d) == 0 {
			d = E.DetectorMinus(id)
		}

		if len(q) > 0 && len(d) > 0{
			break
		}

		time.Sleep(E.TICK)
	}	

	// Now assume q meets detector, we break the entanglement
	// refusing new updates to stabilize the state for measurement
	// This improves the accuracy of the result by O(0.01)

	E.StopAccepting(id) 

	// Stable measurement now possible at this end

	eigenvalue,_ := E.DetectorInteraction(id,q,d)

	return eigenvalue
}

