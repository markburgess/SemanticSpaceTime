
package main

import (
	"fmt"
	S "SST"
)

// ****************************************************************************

func main() {

	S.InitializeSmartSpaceTime()

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	g := S.OpenAnalytics(dbname,url,user,pwd)

	// more	

	n1 := S.CreateNode(g,"test1","This is a long data string which is NOT inappropriate to use as a key",1.5)

	fmt.Println("Now check the node test1 again to see the changes", n1)
}

// ****************************************************************************