
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
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
		path = fmt.Sprintf("/home/mark/.bashrc")
		fmt.Println("No file specified, using",path)
	}

	ShowFile(path)

}

//**************************************************************

func ShowFile(pathname string) {

	fmt.Println("Listing files in",pathname)

	contents, err := ioutil.ReadFile(pathname)

	if err != nil {
		fmt.Println("Couldn't read file in ",pathname,err)
		os.Exit(1)
	}

	var n int = 0

	lines := strings.Split(string(contents),"\n")

	for l := range lines {

		fmt.Println("line", n, "( len =", len(lines[l]), ")", lines[l])
		n++
	}

}