//
// Deriving semantic keys for spacetime
//

package main

import (
	"fmt"
	"time"
	"runtime"
)

// ****************************************************************************

func main() {

	text, key := Now()

	fmt.Println("Time:", text,"\n")

	fmt.Println("Convergent time key:", key,"\n")

	fmt.Println("Location:", Here(1),"\n")


}

// ****************************************************************************

var GR_DAY_TEXT = []string{
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    }
        
var GR_MONTH_TEXT = []string{
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
}
        
var GR_SHIFT_TEXT = []string{
        "Night",
        "Morning",
        "Afternoon",
        "Evening",
    }

// ****************************************************************************

func Now() (string,string) {

	// Time

	then := time.Now()

	year := fmt.Sprintf("Yr%d",then.Year())
	month := GR_MONTH_TEXT[int(then.Month())-1]
	day := then.Day()
	hour := fmt.Sprintf("Hr%02d",then.Hour())
	mins := fmt.Sprintf("Min%02d",then.Minute())
	quarter := fmt.Sprintf("Q%d",then.Minute()/15 + 1)
	shift :=  fmt.Sprintf("%s",GR_SHIFT_TEXT[then.Hour()/6])

	//secs := then.Second()
	//nano := then.Nanosecond()

	dayname := then.Weekday()
	dow := fmt.Sprintf("%.3s",dayname)
	daynum := fmt.Sprintf("Day%d",day)

        interval_start := (then.Minute() / 5) * 5
        interval_end := (interval_start + 5) % 60
        minD := fmt.Sprintf("Min%02d_%02d",interval_start,interval_end)

	var when string = fmt.Sprintf("%s,%s,%s,%s,%s at %s %s %s %s",shift,dayname,daynum,month,year,hour,mins,quarter,minD)

	var key string = fmt.Sprintf("%s:%s:%s",dow,hour,minD)

	return when, key
}

// ****************************************************************************

func Here(depth int) string {

        // Interal usage
	p,fname,line, ok := runtime.Caller(depth)
	
	var location string

	if ok {
		var funcname = runtime.FuncForPC(p).Name()
		fn := "function "+funcname
		file := "file "+fname
		lnr := fmt.Sprintf("line %d",line)
		location = fmt.Sprintf(" in %s of %s at %s",fn,file,lnr)
		
	} else {
		location = "unknown origin"
	}
	
	return location
}



