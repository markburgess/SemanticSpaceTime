

package main

import (
	"fmt"  // go is package based, so you have to import everything here
)

// ****** comments like this ******

func main () {

	// yes, we MUST place the curly braces in this ugly K&R style :(

	var hello string = "Goodbye,"
	
	my := "implicitly typed"

	// call by: package.function()

	fmt.Println(hello,my,"world")

}