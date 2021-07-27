
package main

import (
	"fmt"
	S "SST"
)

// ****************************************************************************

func main() {

	fmt.Println("Prime Semantic Spacetime - set up database model")
	
	S.InitializeSmartSpaceTime()

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	// Do your own stuff

	g := S.OpenAnalytics(dbname,url,user,pwd)

	n1 := S.CreateNode(g,"test1","This is a long data string which is inappropriate to use as a key",0.4)
	n2 := S.CreateNode(g,"test2","This is another long data string which is inappropriate to use as a key", 1.3)

	S.NodeLink(g,n1,"IS_LIKE",n2)
}

// ****************************************************************************