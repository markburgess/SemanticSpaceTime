
package main

import (
	"fmt"
	"os"
	"context"
	"time"
	C "CAIDA_SST"
	A "github.com/arangodb/go-driver"

)

// ********************************************************************************

const max_hop_radius = 20  // Smaller radius as there is no long range order

// ********************************************************************************

func main() {
	
	var dbname string = "ITDK-snapshot-model"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g C.ITDK
	
	g = C.OpenITDK(dbname, service_url, user, pwd)

	LinkAS(g)

}

// ********************************************************************************

func LinkAS(g C.ITDK) {

	var err error
	var cursor A.Cursor

	// count links into Near for AS to AS correlations for each device node member in the AS

	qstring := "FOR dev IN Near FILTER dev.semantics == 'ADJ_NODE' FOR as1 IN Contains FILTER as1._to LIKE 'AS/%' && as1._from == dev._from FOR as2 IN Contains FILTER as2._to LIKE 'AS/%' && as2._from == dev._to && as1._id != as2._id UPSERT { _from: as1._to, _to: as2._to } INSERT {  _from: as1._to, _to: as2._to, semantics: 'AS_ADJ' , weight: '1.0'} UPDATE { weight: OLD.weight + 1.0  } INTO Near"

	// This might take a long time, so we need to extend the timeout
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Hour*8))
	
	defer cancel()
	
	cursor,err = g.S_db.Query(ctx,qstring,nil)
	
	if err != nil {
		fmt.Printf("Query failed: %v", err)
		os.Exit(1)
	}
	
	defer cursor.Close()
	
	for {
		var count int
		
		_,err = cursor.ReadDocument(nil,&count)
		
		if A.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			fmt.Printf("Doc returned: %v", err)
		} else {
			//fmt.Println("hop_radius",hop_radius,"count",count, effvolume[hop_radius])
		}
	}
}
