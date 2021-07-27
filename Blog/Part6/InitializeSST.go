
package main

import (
	"fmt"
	S "SST"
)

// ****************************************************************************

func main() {

	fmt.Println("Prime Semantic Spacetime - set up database model")
	
	S.InitializeSmartSpaceTime()

	// Test nodes

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	n1 := S.CreateNode(g,"test1","This is a long data string which is inappropriate to use as a key")
	n2 := S.CreateNode(g,"test2","This is another long data string which is inappropriate to use as a key")

	S.NodeLink(g,n1,"IS_LIKE",n2)
}

// ****************************************************************************