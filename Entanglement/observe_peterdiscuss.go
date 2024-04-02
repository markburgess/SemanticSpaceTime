
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

	var Ecsh = make(map[string]float64,0)
	var Rcsh = make(map[string]float64,0)

	for theta_L := -thetaL; theta_L <= thetaL; theta_L += 90 {
		for theta_R := -thetaR; theta_R <= thetaR; theta_R += 45 {

			E,Delta,Pyy,Pnn,Pyn,Pny,R,rDelta,Ryy,Rnn,Ryn,Rny := Trial(theta_L,theta_R)

			key := fmt.Sprintf("%.0f,%.0f",theta_L,theta_R)
			Ecsh[key] = E
			Rcsh[key] = R

			fmt.Printf("(%6.1f,%6.1f) %6.3f pm %.3f (%6.3f) : Pyy=%4.2f,Pnn=%4.2f,Pyn=%4.2f,Pny=%4.2f CMP %6.3f pm %6.3f : Ryy=%4.2f,Rnn=%4.2f,Ryn=%4.2f,Rny=%4.2f ->(%s)\n",theta_L,theta_R,E,Delta, -math.Cos((theta_L-theta_R)/360.0*2*3.14),Pyy,Pnn,Pyn,Pny,R,rDelta,Ryy,Rnn,Ryn,Rny,key)
		}
	}

	qm := Ecsh["0,45"] + Ecsh["0,-45"] + Ecsh["90,45"] - Ecsh["90,-45"]
	cm := Rcsh["0,45"] + Rcsh["0,-45"] + Rcsh["90,45"] - Rcsh["90,-45"]

	fmt.Printf("Compare: QM = %f, CM = %f\n",qm,cm)
}

// ****************************************************************************

func Trial(thetaL,thetaR float64) (float64,float64,float64,float64,float64,float64,float64,float64,float64,float64,float64,float64) {

	// Sampling affinity for q and qbar

	var P,Delta,pyy,pnn,pyn,pny float64
	var N,Nyy,Nnn,Nyn,Nny int = 0,0,0,0,0

	var rP,rDelta,ryy,rnn,ryn,rny float64
	var Ryy,Rnn,Ryn,Rny int = 0,0,0,0

	for rounds := 0; rounds < max_rounds; rounds++ {

		E.NewPair()

		// wait for equilibrium (don't set noise rate too high or distance too short)

		time.Sleep(E.EQUILIBRATE)

		// These can't be simultaneous but within the stability window

		E.SetDetector("L",thetaL)
		E.SetDetector("R",thetaR)

		eL,rL := Observe("L")  // if we insert a delay here, does nothing because we've assumed
		eR,rR := Observe("R")  // linear motion doesn't affect the spin phase, but the particles
  		                       // continue to share a clock, spin isn't a fn of momentum

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

		// Compare
		
		if rL < 0 && rR > 0 {
			Rny += (-rL*rR)*(-rL*rR)
		}
		
		if rL > 0 && rR < 0 {
			Ryn += (-rL*rR)*(-rL*rR)
		}
		
		if rL > 0 && rR > 0 {
			Ryy += (rL * rR) * (rL * rR)
		}
		
		if rL < 0 && rR < 0 {
			Rnn += (rL * rR) * (rL * rR)
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

		RTOT := float64(Ryy+Rnn+Ryn+Rny)
		rP = float64(Ryy+Rnn-Ryn-Rny)/RTOT

		ryy = float64(Ryy)/RTOT
		rnn = float64(Rnn)/RTOT
		ryn = float64(Ryn)/RTOT
		rny = float64(Rny)/RTOT
	}
	
	// Error estimate
	
	Delta = math.Sqrt((pyy-pnn)*(pyy-pnn) + (pny-pyn)*(pny-pyn))
	rDelta = math.Sqrt((ryy-rnn)*(ryy-rnn) + (rny-ryn)*(rny-ryn))
	
	return P,Delta,pyy,pnn,pyn,pny,rP,rDelta,ryy,rnn,ryn,rny
}

//***********************************************************

func Observe(id string) (int,int) {
	
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
	// This ought to be irrelevant. It's the eigenvalue collapse from phase 
	// that stabilizes, as long as the phase doesn't change during measurement 
	// (which we thus assume it can't)

	E.StopAccepting(id) 

	// Stable measurement now possible at this end

	eigenvalue := E.DetectorInteraction(id,q,d)
	amplitude  := E.ClassicalDetectorInteraction(id,q,d)

	return eigenvalue,amplitude
}
