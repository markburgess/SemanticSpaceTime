
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
)

//**************************************************************

func main() {

	flag.Parse()
	args := flag.Args()

	fmt.Println("Command line with",len(args),"arguments")

	var path string

	if len(args) == 1 {

		path = args[0]

	} else {
		path = fmt.Sprintf("/home/mark")
		fmt.Println("No directory specified, using",path)
	}

	ListDirectory(path)

//	description, err := ioutil.ReadFile(descr)

}

//**************************************************************

func ListDirectory(pathname string) {

	fmt.Println("Listing files in",pathname)

	files, err := ioutil.ReadDir(pathname)

	if err != nil {
		fmt.Println("Couldn't read file lists in ",pathname,err)
		os.Exit(1)
	}

	var n int = 0

	for _, file := range files {

		info, err := os.Stat(pathname+"/"+file.Name())

		if err != nil {
			fmt.Println("Couldn't stat ",file.Name(), err)
			continue
		}

		if info.IsDir() {
			fmt.Println(n,file.Name(),"(directory/collection)")
		} else {
			fmt.Println(n,file.Name(),"(file/document)")
		}

		n++
	}
}
