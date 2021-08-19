
package main

import (
	"fmt"
	S "SST"
)

// ****************************************************************************

func main() {

	// Prologue

	S.InitializeSmartSpaceTime()

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	// Do your own stuff

	n1 := S.CreateNode(g,"test1","This is a long data string which is inappropriate to use as a key",0.4)
	n2 := S.CreateNode(g,"test2","This is another long data string which is inappropriate to use as a key", 1.3)

	S.CreateLink(g,n1,"IS_LIKE",n2, 55)

	fmt.Println("Now check the links in collection \"NEAR\" using the browser")
}

// ****************************************************************************