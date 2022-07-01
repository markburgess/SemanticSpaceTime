
package Entangle

import (
//	"fmt"
	"os"
	"time"
	"math/rand"
)

const dt = 10  * time.Millisecond  // no less than 10 else write failure

const WAVE = "uuuu0000dddd0000"
const WAVELENGTH = len(WAVE)

var TICK = dt
var EQUILIBRATE = dt * 10
var ERROR_RATE = 4000     // not too great, or too much noise for a stable result

var S = rand.NewSource(time.Now().UnixNano())
var R = rand.New(S)

//***********************************************************

func NewPair() {  // signal start of new pair
	
	/* here we design the interior states of the particle pair 
           there should be enough degrees of freedom to have wavelike 
           behaviour, leading to interference let 1 = +1 and 0 = -1 */

	q,qbar := RandomState()

	//fmt.Println("NEW PAIR q",string(q),string(qbar))

	switch GetRandUpDown() {

	case 1: 
		QPlus("L",q)
		QPlus("R",qbar)
	case -1:
		QPlus("R",q)
		QPlus("L",qbar)
	}

	// Signal one is local at each end - am I willing to accept from other?

	StartAccepting("L")
	StartAccepting("R")
}

//***********************************************************

func OneEndDetected(id string) bool {

	_, err := os.ReadFile("/tmp/accepting"+id)

	return err == nil
}

//***********************************************************

func DetectorInteraction(id string, q,d []byte) (int,float64) {

	angle := PhaseShifted(q,d)
	eigenvalue := 0

	convolution := make([]byte,WAVELENGTH)

	for pos := 0; pos < WAVELENGTH; pos++ {
		convolution[pos] = Qbit(Qval(q[pos]) * Qval(d[pos]))
		switch convolution[pos] {
		case 'u': eigenvalue = 1
		case 'd': eigenvalue = -1
		}
	}

	return eigenvalue, angle
}

//***********************************************************

func PhaseShifted(q,d []byte) float64 {

	posq := GetLeadingEdge(q)
	posd := GetLeadingEdge(d)

	angle := Orientation(float64(posq-posd)/float64(WAVELENGTH) * 360.0)

	//fmt.Println(string(q),string(d),"shift",angle)

	return angle
}

// *************************************************************

func GetLeadingEdge(process []byte) int {

	for pos := 0; pos < WAVELENGTH; pos++ {

		if process[Cyc(pos-1)] == '0' {

			switch process[pos] {
				
			case 'u':
				return pos
			case 'd': 
				return Cyc(WAVELENGTH/2+pos)
			}
		}
	}

	return 0
}

//***********************************************************

func RandomState() ([]byte,[]byte) {
	
	q := make([]byte,WAVELENGTH)

	offsetL := GetRandOffset()

	for delta := 0; delta < WAVELENGTH; delta ++ {

		posL := (delta + offsetL) % WAVELENGTH
		q[posL] = WAVE[delta]
	}

	return q,Bar(q)
}

//***********************************************************

func Bar(q []byte) []byte {

	qbar := make([]byte,WAVELENGTH)

	for delta := 0; delta < WAVELENGTH; delta++ {

		qbar[delta] = Complement(q[delta])
	}

	return qbar
}

//***********************************************************

func Complement(q byte) byte {
	
	switch q {
		
	case 'u':  
		return byte('d')
	case 'd': 
		return byte('u')
	}
	
	return byte('0')
}

//***********************************************************

func GetRandUpDown() int {

	var q int

	for q = R.Intn(3)-1; q != -1 && q != 1; q = R.Intn(3)-1 {
	}

	return q
}

//***********************************************************

func GetRandOffset() int {

	return R.Intn(WAVELENGTH)
}

//***********************************************************

func StartAccepting(id string) { // absorb one end

	//fmt.Println("Emit, start entanglement",id)
	os.WriteFile("/tmp/accepting"+id, []byte("GOGOGO"), 0644)
	time.Sleep(dt)
}

//***********************************************************

func StopAccepting(id string) { // absorb one end

	//fmt.Println("Absorbed, break entanglement",id)
	os.Remove("/tmp/accepting"+id)
	time.Sleep(dt)
}


//***********************************************************

func Abs(i int) int {

	if i >= 0 {
		return i
	}

	return -i
}

//***********************************************************

func Qval(b byte) int {

	switch b {

	case 'u': return 1
	case '0': return 0
	case 'd': return -1
	}

return 0
}

//***********************************************************

func Qbit(b int) byte {

	switch b {

	case 1: return 'u'
	case 0: return '0'
	case -1: return 'd'
	}

return 0
}

//***********************************************************

func Undefined(q []byte) bool {

	if q == nil || len(q) < WAVELENGTH {
		return true
	}

	for i := 0; i < WAVELENGTH; i++ {

		if q[i] != 'u' && q[i] != 'd' && q[i] != '0'  {
			return true
		}
	}

return false
}

//***********************************************************

func Cyc(pos int) int {

	if pos < 0 {
		return (2 * WAVELENGTH + pos) % WAVELENGTH
	} else {
	
		return pos % WAVELENGTH
	}
}

//***********************************************************

func Orientation(angle float64) float64 {

	a := int(angle)

	if a < 0 {
		return float64((360 + a) % 360)
	} else {
	
		return float64(a % 360)
	}
}

//***********************************************************

func SetDetector(id string, offset float64) {

	edge := int(offset/360.0 * float64(WAVELENGTH))

	// Find offset, supporting +1.0 and -1.0 interval
	// Invariance under phase shifts means these shifts are independent of L,R

	d := make([]byte,WAVELENGTH)

	for pos := 0; pos < WAVELENGTH; pos++ {
		
		d[Cyc(pos+edge)] = WAVE[pos]
	}

	os.WriteFile("/tmp/D"+id, []byte(d), 0644)
}

//***********************************************************

func DetectorMinus(id string) []byte {

	detector := "/tmp/D"+id
	
	d, _ := os.ReadFile(detector)

	return d
}

//***********************************************************

func QMinus(id string) []byte {

	var idbar string

	switch id {
		
	case  "L": 
		idbar = "R"
		
	case  "R": 
		idbar = "L"
	}

	inchannel := "/tmp/channel" + idbar

	q, _ := os.ReadFile(inchannel)

	return q
}

//***********************************************************

func QPlus(id string, q []byte) {

	outchannel := "/tmp/channel" + id
	os.WriteFile(outchannel, []byte(q), 0644)

	// Need to wait for disk write to equilibrate, 
	// else undefined read will fail, no less than 5 here

	time.Sleep(5*TICK)

}