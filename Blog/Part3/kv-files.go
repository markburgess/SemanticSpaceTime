//
// A more sophisticated / idempotent approach to writing data
// Standalone, nothing else needed

// ************************************************************

package main

import (
	"fmt"
	"os"
	"io/ioutil"
)

// ************************************************************

type IntKeyValue struct {

	K  string `json:"_key"`
	V  int    `json:"value"`
}

// ************************************************************

func main() {

	fmt.Println("Create a key value lookup table")

	var dbname string = "/tmp/SemanticSpacetime"

	db := OpenFakeDatabase(dbname)

	// Create documents

	kv := []IntKeyValue{

		IntKeyValue{
			K: "NEAR",
			V:  1,
		},

		IntKeyValue{
			K: "FOLLOWS",
			V:  2,
		},

		IntKeyValue{
			K: "CONTAINS",
			V:  3,
		},

		IntKeyValue{
			K: "EXPRESSES",
			V:  4,
		},

	}

	// Add to DB

	SaveIntKVMap("ST_Types_Map",db,kv)

	// Retrieve from DB

	PrintIntKV(db,"ST_Types_Map")

	// Import constant lookup table from DB

	var const_STtype = make(map[string]int)

	LoadIntKV2Map(db,"ST_Types_Map", const_STtype)

	fmt.Println("RESULT 1 (corrected values): ",const_STtype)

	IncrementIntKV(db,"ST_Types_Map","EXPRESSES")
	LoadIntKV2Map(db,"ST_Types_Map", const_STtype)

	fmt.Println("RESULT 2 (incremented values): ",const_STtype)

	fmt.Println("Using const_STtype[\"CONTAINS\"] as a named constant/invariant: ",const_STtype["CONTAINS"])
}

//********************************************************
// Toolkit
//********************************************************

func OpenFakeDatabase(name string) string {

	var directory_exists bool

	directory_exists = DirectoryExists(name)

	if directory_exists {
		
		fmt.Println("Opening existing database")

	} else {

		MakeDir(name)
	}

	return name
}

//********************************************************

func SaveIntKVMap(coll_name string, db string, kv []IntKeyValue) {

	// Create collection

	var coll_exists bool

	coll_path := db + "/" + coll_name

	coll_exists = DirectoryExists(coll_path)

	if coll_exists {

		fmt.Println("Collection " + coll_name +" exists already")

	} else {

		MakeDir(coll_path)
	}

	for k := range kv {

		AddIntKV(coll_path, kv[k])
	}
}

// **************************************************

func PrintIntKV(db string, coll_name string) {

	coll_path := db + "/" + coll_name

	docs, err := ioutil.ReadDir(coll_path)
	
	if err != nil {
		fmt.Println("Couldn't read directory ",coll_path,err)
		os.Exit(1)
	}
	
	for _, doc := range docs {

		if doc.Mode().IsRegular() {

			var kv IntKeyValue
			var doc_path string = coll_path + "/" + doc.Name()

			text, err := ioutil.ReadFile(doc_path)
			
			if err != nil {
				fmt.Println("Couldn't read link " + doc_path)
				os.Exit(1)
			}

			kv.K = doc.Name()

			fmt.Sscanf(string(text),"%d", &kv.V)
			fmt.Print("(K,V): (",kv.K,",", kv.V,") \n")
		}
	}
}

// **************************************************

func AddIntKV(coll string, kv IntKeyValue) {

	// Add data with convergent semantics, CFEngine style
	// The filename is the key

	var coll_path = coll + "/" + kv.K

	exists := FileExists(coll_path)

	if !exists {

		fmt.Println("Adding/Restoring",kv)

		// Write file name kv.K with content kv.V which can't be string

		text := []byte(fmt.Sprintf("%d",kv.V))

		err := ioutil.WriteFile(coll_path,text, 0644)

		if err != nil {
			fmt.Printf("Failed to write value: %s %v",kv.K,err)
			os.Exit(1);
		}
	} else {
		
		var checkkv IntKeyValue
		
		text,err := ioutil.ReadFile(coll_path)

		fmt.Sscanf(string(text),"%d", &checkkv.V)

		if checkkv.V != kv.V {
			fmt.Println("Correcting data",checkkv,"to",kv)

			content := []byte(fmt.Sprintf("%d",kv.V))
			err = ioutil.WriteFile(coll_path, content, 0644)

			if err != nil {
				fmt.Printf("Failed to update value: %s %v",kv.K,err)
				os.Exit(1);
			}
		}
	}
}

// **************************************************

func IncrementIntKV(db string, coll_name, key string) {

	var kv,checkkv IntKeyValue
	var doc_path = db + "/" + coll_name + "/" + key

	text,err := ioutil.ReadFile(doc_path)
	fmt.Sscanf(string(text),"%d", &checkkv.V)

	fmt.Println("Incrementing data",checkkv.V,"to",kv.V)

	kv = checkkv
	kv.V++

	// update
	
	content := []byte(fmt.Sprintf("%d",kv.V))
	err = ioutil.WriteFile(doc_path, content, 0644)
	
	if err != nil {
		fmt.Printf("Failed to update value: %s %v",kv.K,err)
		os.Exit(1);
	}
}

// **************************************************

func LoadIntKV2Map(db string, coll_name string, extkv map[string]int) {

	var err error
	var coll_path = db + "/" + coll_name

	docs, err := ioutil.ReadDir(coll_path)
	
	if err != nil {
		fmt.Println("Couldn't read directory ",coll_path,err)
		os.Exit(1)
	}
	
	for _, doc := range docs {

		if doc.Mode().IsRegular() {

			var kv IntKeyValue
			var doc_path string = coll_path + "/" + doc.Name()

			text, err := ioutil.ReadFile(doc_path)
			
			if err != nil {
				fmt.Println("Couldn't load key " + doc_path)
				os.Exit(1)
			}

			kv.K = doc.Name()

			fmt.Sscanf(string(text),"%d", &kv.V)
			extkv[kv.K] = kv.V
		}
	}
}

// ****************************************************************************

func FileExists(path string) bool {
	
	info, err := os.Stat(path)
	
	if err == nil  && info.Mode().IsRegular() { return true }
	if os.IsNotExist(err) { return false }
	return true
}

// ****************************************************************************

func DirectoryExists(path string) bool {

	info, err := os.Stat(path)
	if err == nil && info.IsDir() { return true }
	if os.IsNotExist(err) { return false }
	return true
}

//**************************************************************

func MakeDir(pathname string) {

	err := os.MkdirAll(pathname, 0755)

	if err != nil {
		fmt.Println("Couldn't make directory " + pathname)
		os.Exit(1)
	}

	// Don't fail if the directory is ok

	if err == nil || os.IsExist(err) {
		return
	} else {
		fmt.Println("Couldn't makedir ",pathname,err)
		os.Exit(1)
	}
}
