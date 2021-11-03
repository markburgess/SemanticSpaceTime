
package main

import (
	"fmt"
	"flag"
	"os"
	"bufio"
	"strings"
	"strconv"
	"net"
	"sort"

	C "CAIDA_SST"
)

// ********************************************************************************

const MAXLINES = 500000 // real    123m49.225s

// Data files from https://publicdata.caida.org/datasets/topology/ark/ipv4/itdk/2020-08/

const DEVICES = "midar-iff.nodes"
const GEO = "midar-iff.nodes.geo"
const LINKS = "midar-iff.links"
const AS = "midar-iff.nodes.as"
const DNS = "itdk-run-20200819-dns-names.txt"

//?
const IFACES = "midar-iff.ifaces"

// ********************************************************************************

func main() {
	
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	
	if len(args) < 1 {
		fmt.Println("Directory name for input files expected")
		os.Exit(1);
	}
	
	var path string = args[0]
	
	var dbname string = "ITDK-snapshot-model"
	var service_url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"
	
	var g C.ITDK
	
	g = C.OpenITDK(dbname, service_url, user, pwd)

	// Load the files one by one

	fmt.Println("Processing .nodes file")
	ProcessFileByLines(g,path + "/" + DEVICES, AddDevices)

	fmt.Println("Processing .links file")
	ProcessFileByLines(g,path + "/" + LINKS, AddLinks)

	fmt.Println("Processing .as file")
	ProcessFileByLines(g,path + "/" + AS, AddAS)

	fmt.Println("Processing .geo file")
	ProcessFileByLines(g,path + "/" + GEO, AddGeo)

	fmt.Println("Processing .dns file")
	ProcessFileByLines(g,path + "/" + DNS, AddDNS)
	
}

// ********************************************************************************

func usage() {

	fmt.Println("go run itdk.go <directory containing data files>")
}

// ********************************************************************************

func AddDevices(g C.ITDK, linenumber int, line string) {

	// Format: node <node_id>:   i1 i2 [i3] ..
        // These interfaces belong to a single device, see
        // https://www.caida.org/catalog/papers/2011_midar_tr/midar-tr.pdf

	list := strings.Split(string(line)," ")

	device,_,_ := GetDeviceWithIP(g,list[1])

	for i := 2; i < len(list); i++ {

		ipaddr2,ipnode2 := GetIP(g,list[i])

		if ipaddr2 == "" {
			continue
		}

		C.CreateLink(g,device,"HAS_INTERFACE",ipnode2,0)
	}
}

// ********************************************************************************

func AddLinks(g C.ITDK, linenumber int, line string) {

	//  Format: link <link_id>:   <N1>:i1   <N2>:i2   [<N3>:[i3] .. [<Nm>:[im]]
	//  Example: link L104:   N242484:211.79.48.158 N1847:211.79.48.157 N5849773
        //           [0]  [1]  [2][3]                   [4] ...

	list := strings.Split(string(line)," ")

	// This isn't too robust...
	// list[0] == "link"
	// list[1]  don't need this?
	// list[2] is an additional stray space
        // list[3] is receiver, all others are source routes to receiver 

	recv_node := list[3]

	device_recv , ipaddr_recv, ipnode_recv := GetDeviceWithIP(g,recv_node)

	// All the rest are connections ... 
        // if more than one (len list > 4) there must be an unknown intermediary

	if len(list) == 3 {

		device_neigh, ipaddr_neigh, ipnode_neigh := GetDeviceWithIP(g,list[3])

		// Annotate the link with the IP addresses if known, else ambiguous

		C.CommentedLink(g,device_recv,"ADJ_NODE",device_neigh,ipaddr_recv,ipaddr_neigh,0)

		if ipaddr_recv != "" && ipaddr_neigh != "" && ipaddr_recv != ipaddr_neigh {

			C.CreateLink(g,ipnode_recv,"ADJ_IP",ipnode_neigh,0)
		}

		return
	}

	// >3 devices, implies some intermediate hub (switch, MPLS cloud, etc)

	name := InventName(device_recv.Key,list,3)      // combine names from 3 ..
	unkn := C.CreateUnknown(g,name)

	// Two link types express the same connection, with different semantics
	// to enable selective searches

	C.CommentedLink(g,device_recv,"ADJ_UNKNOWN",unkn,ipaddr_recv,"*",0)
	C.CommentedLink(g,device_recv,"ADJ_NODE",unkn,ipaddr_recv,"*",0)

	for i := 4; i < len(list); i++ {

		if len(list[i]) < 2 {
			continue
		}

		device_neigh, ipaddr_neigh, _ := GetDeviceWithIP(g,list[i])

		// Annotate the link with the IP addresses if known, else ambiguous

		C.CommentedLink(g,device_neigh,"ADJ_UNKNOWN",unkn,ipaddr_neigh,"*",0)
		C.CommentedLink(g,device_neigh,"ADJ_NODE",unkn,ipaddr_neigh,"*",0)
	}

}

// ********************************************************************************

func AddAS(g C.ITDK, linenumber int, line string) {

	//  Format: node.AS <node_id>:..<AS> <method>
        //  Use the Comment field for method

	var device, AS, method string
	var n,a C.Node

	fmt.Sscanf(line,"node.AS %s %s %s",&device,&AS,&method)

	device = strings.Trim(device,":")

	a = C.CreateAS(g,AS,method)
	n = C.CreateDevice(g,device)

	C.CreateLink(g,n,"PART_OF",a,0)
}

// ********************************************************************************

func AddGeo(g C.ITDK, linenumber int, line string) {

	//  Format: this uses tabs, and there are some malformed lines

	line = strings.TrimLeft(line,"node.geo")
	line = strings.TrimLeft(line," ")
	line = strings.TrimLeft(line,"\t")

	list := strings.Split(string(line),"\t")

        device_set := strings.Trim(list[0],":")
	//continent := list[1]
	country := list[2]
	region := list[3]
	city := list[4]
	lat,_ := strconv.ParseFloat(list[5],64)
	long,_ :=  strconv.ParseFloat(list[6],64)

	if len(country) == 0 {
		fmt.Println("Skipping rogue line:",linenumber,line)
		for i := range list {
			fmt.Println("   ",i,list[i])
		}
		return
	}

	n := C.CreateDevice(g,device_set)
	c := C.CreateCountry(g,country)
	r := C.CreateRegion(g,region,city,lat,long)

	C.CreateLink(g,n,"ASET_IN",r,0)
	C.CreateLink(g,r,"REGION_IN",c,0)
}

// ********************************************************************************

func AddDNS(g C.ITDK, linenumber int, line string) {

	//  Format: this uses tabs

	list := strings.Split(string(line),"\t")

	// list[0] == node.geo

	if len(list[1]) > 0 && len(list[2]) > 0 {

		_,ip := GetIP(g,list[1])
		domain := list[2]

		dom := C.CreateDomain(g,domain)
		C.CreateLink(g,dom,"HAS_ADDR",ip,0)
	}
}

// ****************************************************************************
// Helpers
// ****************************************************************************

func ProcessFileByLines(g C.ITDK,filename string,process_function func(C.ITDK,int,string)) {

	var marker int

	if MAXLINES > 100000 {
		marker = 10000
	} else {
		marker = MAXLINES / 10
	}

	file, err := os.Open(filename)

	if err != nil {
		fmt.Printf("error opening file: %v\n",err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var line string
	var count int = 0 // indices start at 0 in the files
 
	for scanner.Scan() {
		line = scanner.Text()

		if line[0] == '#' || len(line) < 8 {
			count++
			continue
		}

		process_function(g,count,line)

		count++

		if count % marker == 0 {
			fmt.Println(count,"...")
		}

		if count > MAXLINES {
			break
		}
	}
 
	file.Close()
}

// ****************************************************************************

func GetIP(g C.ITDK, s string) (string,C.Node) {

	ipaddr := net.ParseIP(s)

	var ip C.Node
	
	if ipaddr != nil {

		if ipaddr.To4() == nil {
			ip = C.CreateIPv6(g,s)
		} else {
			ip = C.CreateIPv4(g,s)
		}
	}

	return s, ip
}

// ****************************************************************************

func GetDeviceWithIP(g C.ITDK, s string) (C.Node,string,C.Node) {

	var device,ip C.Node
	var ipaddr net.IP
	
	array := strings.Split(s,":")

	id := array[0]

	device = C.CreateDevice(g,id)

	if len(array) > 1 {
		ipaddr = net.ParseIP(array[1])
		
		if ipaddr != nil {
			
			if ipaddr.To4() == nil {
				ip = C.CreateIPv6(g,array[1])
			} else {
				ip = C.CreateIPv4(g,array[1])
			}
		}
	}

	return device, string(ipaddr), ip
}

// ****************************************************************************

func InventName(src string, list []string, offset int) string {

	// Make a composite hub name, e.g. _N110915_N24129039_N24129039

	var name string = ""
	var parts []string = make([]string,0)
	var n int = 0

	parts = append(parts,src)

	for i := offset; i < len(list); i++ {

		// Strip off any trailing IP addresses
		s := strings.Split(list[i],":")
		n++
		parts = append(parts,s[0])
	}

	// Avoid too much duplication by re-ordering

	sort.Strings(parts)

	for n = 0; n < len(parts); n++ {
		name += "_" + parts[n]
	} 

	full := C.InvariantDescription(name)

	// Names have to be less than 250 chars

	var short string

	if len(full) > 250 {
		short = full[:250]
	} else {
		short = full
	}

	return short
}