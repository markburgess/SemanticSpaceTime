//
// Copyright © Mark Burgess, ChiTek-i (2020)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"strings"
	"os"
	"io/ioutil"
	"context"
	"flag"
	"fmt"
	"regexp"
	"path"
	"sort"
	S "SST"
)

// ****************************************************************************

// THIS IS THE CODE USED IN PAPER 2

// Look for the invariants

// Invariants - looks for interferometry of fragments -- persistent sequences
// over consecutive legs. This helps to stabilize conceptual fragments - more
// certain if they are repeated.
// Measured by sentence number, not just the selected few.

// In this expt we reduced the threshold for meaning so collecting more events
// higher density

// ****************************************************************************

// Short term memory class

type Narrative struct {

	rank float64
	text string
	context string
	index int
}

const MAXCLUSTERS = 7
const LEG_WINDOW = 100

var WORDCOUNT int = 0
var LEGCOUNT int = 0

var KEPT int = 0
var SKIPPED int = 0

var TOTAL_PARAGRAPHS int = 0
var ALL_SENTENCE_INDEX int = 0

var SENTENCES []Narrative

var THRESH_ACCEPT float64 = 0
var TOTAL_THRESH float64 = 0
var MAX_IMPORTANCE float64

// Relating to relative concentrations of memory LTM

const REPEATED_HERE_AND_NOW  = 1.0
const INITIAL_VALUE = 0.5
const MEANING_THRESH = 20  // reduce this if too few samples

// ****************************************************************************
// The ranking vectors for structural objects in a narrative
// LHS = type (semantic, metric) and RHS = importance / relative meaning
// ****************************************************************************

// n-phrase clusters are semantic units (no relevant order) - these are memory
// implicated in selection at the "smart sensor" level, i.e. innate adaptation
// about what is retained from the incomning `signal'

var LTM_CLUSTER_FRAG [MAXCLUSTERS]map[int][]string
var STM_CLUSTER_RANK [MAXCLUSTERS]map[string]float64

var LTM_FRAG_HISTORY [MAXCLUSTERS]map[string][]int

var G S.Analytics

// Another kind of short term memory could be used to detect "co-activation"
// Emotional bias is an example, but how are the dyanmics of this determined?

// 1. FOLLOWS
// 2. PART OF
// 3. EXPRESSES
// 4. NEAR (COACTIVATED/LOCATED) - same section/region

// A generic causal response to "emotional bias"

var ATTENTION_LEVEL float64 = 0.6
var SENTENCE_THRESH float64 = 10

// ****************************************************************************
// SCAN themed stories as text to understand their components
//
//   go run scan_stream.go ~/LapTop/SST/test3.dat 
//
// Version 2 of scan_text using realtime / n-torus approach
//
// We want to input streams of narrative and extract phrase fragments to see
// which become statistically significant - maybe forming a hierarchy of significance.
// Try to measure some metrics/disrtibutions as a function of "amount", where
// amount is measured in characters, words, sentences, paragraphs, since these
// have different semantics.
// ****************************************************************************

func main() {

	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	
	if len(args) < 1 {
		fmt.Println("file list expected")
		os.Exit(1);
	}

	// Init

	for i := 1; i < MAXCLUSTERS; i++ {

		STM_CLUSTER_RANK[i] = make(map[string]float64)
		LTM_CLUSTER_FRAG[i] = make(map[int][]string)
		LTM_FRAG_HISTORY[i] = make(map[string][]int)
	} 
	
	ctx := context.Background()

	S.InitializeSmartSpaceTime()

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	G = S.OpenAnalytics(dbname,url,user,pwd)

	for i := range args {

		if strings.HasSuffix(args[i],".dat") {

			OneDocument(&ctx,args[i])

			SearchInvariants(G)

			SelectSentenceEvents(&ctx,args[i])

		}
	}

	SaveContext(&ctx)

	fmt.Println("\nKept = ",KEPT,"of total ",ALL_SENTENCE_INDEX,"efficiency = ",100*float64(ALL_SENTENCE_INDEX)/float64(KEPT),"%")
	fmt.Println("\nAccepted",THRESH_ACCEPT/TOTAL_THRESH*100,"% into hubs")
}

//**************************************************************
// Start scanning docs
//**************************************************************

func OneDocument(ctx *context.Context, filename string) {

	/// Use the filename as context

	start := strings.ReplaceAll(path.Base(filename),"/",":")

	S.NextDataEvent(&G,start,start)

	TOTAL_PARAGRAPHS = len(Scanfile(ctx, filename))

}

//**************************************************************

func Scanfile(ctx *context.Context, filename string) []string {

	// split each file into paragraphs

	proto_paragraphs := GetFileContent(string(filename))
	
	// split each paragraph into sentences

	for para := range proto_paragraphs {

		ParseOneSmartSensorFrame(ctx, para, proto_paragraphs[para])
	}

return proto_paragraphs
}

//**************************************************************

func ParseOneSmartSensorFrame(ctx *context.Context, p int, paragraph string){

	var sentences []string

	// Coordinatize the non-trivial sentences

	if len(paragraph) > 0 {
		
		sentences = SplitSentences(paragraph)

		for s := range sentences {
			
			srank := RankNPhrases(ALL_SENTENCE_INDEX,sentences[s])

			context := RunningContext()

			if AttentionThreshold(ctx,srank,sentences[s],len(sentences[s])) {

				n := Narrate(sentences[s]+".",srank,context,ALL_SENTENCE_INDEX)
				SENTENCES = append(SENTENCES,n)
			}

			ALL_SENTENCE_INDEX++
		}
	}
}

//**************************************************************

func AttentionThreshold(ctx *context.Context,meaning float64, sen string, senlen int) bool {

	const alert = 1.0
	const awake = 0.5
	const attention_deficit = 0.1
	const sentence_width = 7
	const response = 0.6
	const calm = 0.9

	// If sudden change in sentence length, be alert

	slen := float64(senlen)

	if (slen > SENTENCE_THRESH + sentence_width) {

		ATTENTION_LEVEL = alert
		SENTENCE_THRESH = response * slen + (1-response) * SENTENCE_THRESH
	}

	if (slen < SENTENCE_THRESH - sentence_width) {

		ATTENTION_LEVEL = alert
		SENTENCE_THRESH = response * slen + (1-response) * SENTENCE_THRESH
	}

	// If lots of sentences in this paragraph, skim

	//fmt.Println("MEAN ",meaning)

	if (meaning > MEANING_THRESH) && (ATTENTION_LEVEL > awake) {

		KEPT++

		if ATTENTION_LEVEL > attention_deficit {

			ATTENTION_LEVEL -= attention_deficit
		}

		//fmt.Println("\nKeeping: ", sen)

		return true

	} else {
		
		//fmt.Println("\nSkipping: ", sen)

		SKIPPED++
		return false
	}
}

//**************************************************************

func GetFileContent(filename string) []string {

	var cleaned []string
	var datacheck = make(map[string]int)

	content, _ := ioutil.ReadFile(filename)

	// Start by stripping HTML / XML tags before para-split

	m1 := regexp.MustCompile("<[^>]*>") 
	stripped1 := m1.ReplaceAllString(string(content),"") 

	//Strip and \begin \latex commands

	m2 := regexp.MustCompile("\\\\[^ \n]*") 
	stripped2 := m2.ReplaceAllString(stripped1,"") 

	// Non-English alphabet (tricky)

	m3 := regexp.MustCompile("[{&}“#%^+_#”=$’~‘/()<>\"&]*") 
	stripped3 := m3.ReplaceAllString(stripped2,"") 

	// Treat ? and ! as end of sentence
	m3a := regexp.MustCompile("[?!]+") 
	stripped3a := m3a.ReplaceAllString(stripped3,".") 

	// Strip digits, this is probably wrong in general
	m4 := regexp.MustCompile("[0-9]*")
	stripped4 := m4.ReplaceAllString(stripped3a,"")

	// Close multiple redundant spaces
	m5 := regexp.MustCompile("[ ]+")
	stripped5 := m5.ReplaceAllString(stripped4," ")

	// Now we should have a standard paragraph format but
        // this is format dependent, so add a maximum length limit.
	
	proto_paragraphs := strings.Split(string(stripped5),"\n\n")

	for para := range proto_paragraphs {

		// Remove trailing whitespace
		r := strings.ReplaceAll(proto_paragraphs[para],"\n"," ")
		p := strings.Trim(r,"\n ")

		if len(p) > 0 {
			cleaned = append(cleaned,p)
			datacheck[p]++
		}
	}

	return cleaned
}

//**************************************************************

func SplitSentences(para string) []string {

	// Now split into sentences, with a minimum cutoff

	sentences := strings.Split(para,".")

	// if a sentence contains ,:, we also need to split there even though it refers to the same
	// fmt.Println(" * Sentences",sentences)

	var cleaned []string

	for sen := range sentences{

		// Split first by punctuation marks, because phrases don't cross these boundaries

		f := func(c rune) bool {

			// Something serious going on
			ATTENTION_LEVEL = 1

			return c == ':' || c == ';'
		}

		fields := strings.FieldsFunc(sentences[sen], f)

		for field := range fields {
			content := strings.Trim(fields[field]," ")

			if len(content) > 0 {			
				cleaned = append(cleaned,content)
			}
		}
	}

	return cleaned
}

//**************************************************************

func RankNPhrases(s int, sentence string) float64 {

	var rrbuffer [MAXCLUSTERS][]string
	var sentence_meaning_rank float64 = 0
	var rank float64

	no_dot := strings.ReplaceAll(sentence,"."," ")
	no_comma := strings.ReplaceAll(no_dot,","," ")
	clean_sentence := strings.Split(no_comma," ")

	for word := range clean_sentence {

		// This will be too strong in general - ligatures in xhtml etc

		m := regexp.MustCompile("[^-a-zA-Z0-9]*") 
		cleanjunk := m.ReplaceAllString(clean_sentence[word],"") 
		cleanword := strings.ToLower(cleanjunk)

		if len(cleanword) == 0 {
			continue
		}

		rank, rrbuffer = TickSample(s,cleanword, rrbuffer)
		sentence_meaning_rank += rank
	}

return sentence_meaning_rank
}

//**************************************************************

func SearchInvariants(g S.Analytics) {

	fmt.Println("----- LONGITUDINAL INVARIANTS ----------")

	for i := 1; i < MAXCLUSTERS; i++ {

		for f := range LTM_FRAG_HISTORY[i] {

			if (Irrelevant(f)) {
				continue
			}

			invariance := PersistentCovariant(LTM_FRAG_HISTORY[i][f],100) 

			// This is an arbitrary measure (scale) to allow more long strings and fewer short ones
			// Why would this scale exist?
			// The interferometric aspect is indicative of a noise scale

			if invariance > 20-2*i && Importance(i,f) > float64(2*i) {

				S.UpdateHistogram(G, "Longitudinal",f)
			}
		}
	}

	fmt.Println("-------------")

	// Add the longitudinal invariants as concepts of input language as standalone hubs
	// Then want to attach these to the legs where they occur
	// This should happen automatically from below
}

//**************************************************************

func PersistentCovariant(list []int, radius int) int {

	var count int = 0

	// is the same idea repeated consecutively?

	for i := 0; i < len(list)-2; i++ {

		if (list[i+1] - list[i] < radius) {
			count++
		}
	}

	return count
}

//**************************************************************

func SelectSentenceEvents(ctx *context.Context, filename string) {

	// The importances have now all been measured in realtime, but we review then now...posthoc
	// Now go through the history map chronologically, by sentence only
	// Reset the narrative  `leg' counter when it fills up to measure story progress. 
	// This determines the sampling density of "important sentences"

	var steps,leg int

	const leg_reset = LEG_WINDOW // measured in sentences

	// Sentences to summarize per leg of the story journey

	steps = 0

	var imp_l float64 = 0
	var imp_leg []float64
	
	// First, coarse grain the narrative into `legs', i.e. standardized paragraphs

	for s := range SENTENCES {

		// Sum the importances of each selected sentence

		imp_l += SENTENCES[s].rank

		if steps > leg_reset {
			steps = 0
			leg_importance := imp_l / float64(LEG_WINDOW)
			imp_leg = append(imp_leg,leg_importance)
			imp_l = 0
		}

		steps++	
	}

	// Don't forget the final "short" leg

	leg_importance := imp_l / float64(steps)
	imp_leg = append(imp_leg,leg_importance)

	var max_leg float64 = 0

	for l := range imp_leg {

		if max_leg < imp_leg[l] {

			max_leg = imp_leg[l]
		}
	}

	// Select a sampling rate that's lazy (one sentence per leg) or busy (a few)
	// for important legs

	steps = 0
	leg = 0
	imp_l = imp_leg[0]

	var max_rank = make(map[int]map[float64]int)

	max_rank[0] = make(map[float64]int)

	var context string

	for s := range SENTENCES {

		// Keep the latest running context summary hub, as we go through the sentences

		context = SENTENCES[s].context

		max_rank[leg][SENTENCES[s].rank] = s

		if steps > leg_reset {

			imp_l = imp_leg[leg]

			AnnotateLeg(ctx, filename, leg, max_rank[leg], imp_l, max_leg,context)

			steps = 0
			leg++

			max_rank[leg] = make(map[float64]int)
		}

		steps++
	}

	imp_l = imp_leg[leg]

	AnnotateLeg(ctx,filename,leg,max_rank[leg],imp_l,max_leg,context)
}


//**************************************************************
// TOOLKITS
//**************************************************************

func Importance(n int, s string) float64 {

	// Emotional bias to be added ?

	if _, ok := STM_CLUSTER_RANK[n][s]; !ok {

		return 0
	}

	meaning := float64(len(s)) / (0.5 + STM_CLUSTER_RANK[n][s] )

	if meaning > MAX_IMPORTANCE {
		MAX_IMPORTANCE = meaning
	}

return meaning
}

//**************************************************************

func AnnotateLeg(ctx *context.Context, filename string, leg int, random map[float64]int, leg_imp,max float64, context string) {

	const threshold = 0.8  // 80/20 rule -- CONTROL VARIABLE

	var imp []float64
	var ordered []int

	key := make(map[float64]int)

	for fl := range random {

		imp = append(imp,fl)
	}

	if len(imp) < 1 {
		return
	}

	// Rank by importance

	sort.Float64s(imp)
	context_importance := leg_imp / max

	// The importance level is now almost constant, since we already picked out by attention
	// Get the rank as integer order

	for i := range imp {
		key[imp[i]] = random[imp[i]]
	}

	// Select only the most important remaining in order for the hub
	// Hubs will overlap with each other, so some will be "near" others i.e. "approx" them
	// We want the degree of overlap between hubs S.CompareContexts()

	if context_importance > threshold {

		var start int

		if len(imp) > 3 {
			start = len(imp) - 3
		} else {
			start = 0
		}

		for i :=  start; i < len(imp); i++ {
			
			s := key[imp[i]]
			ordered = append(ordered,s)
		}

		sort.Ints(ordered)

	} else {

		s := key[imp[len(imp)-1]]
		ordered = append(ordered,s)
	}

	// Now in order of importance

	for s := range ordered {

		fmt.Printf("\nEVENT[Leg %d selects %d]: %s\n",leg,ordered[s],SENTENCES[ordered[s]].text)
		//fmt.Printf("\n%s\n",SENTENCES[ordered[s]].text)

		// Connect selected sentences to the context summary HUB in which they occur

		AnnotateSentence(ctx,filename,s,SENTENCES[ordered[s]].text,context,SENTENCES[ordered[s]].index)
	}
}

//**************************************************************

func AnnotateSentence(ctx *context.Context,filename string, s_number int,sentence,context string,index int) {

	// We use the unadulterated sentence itself as an episodic event
	// This acts as an impromptu hub

	prefix := strings.ReplaceAll(path.Base(filename),".","_")

	key := fmt.Sprintf("%s_Sentence_%d",prefix,index)

	event := S.NextDataEvent(&G, key, sentence)

	//fmt.Println("Annotating ", key)
	//S.ContextHub(ctx,"hub:," + context).HubFragment(sentence)

	// Keep the 3-fragments and above that are important enough to pass threshold
	// Then hierarchically break them into words that are important enough.

	// So we have a hierarchy: context_hub - sentence - phrases - significant words

	const min_cluster = 1
	const max_cluster = 4

	for i := min_cluster; i < max_cluster; i++ {

		for f := range LTM_CLUSTER_FRAG[i][index] {
			
			fragment := LTM_CLUSTER_FRAG[i][index][f]
			
			TOTAL_THRESH++
			
			// We can't use Importance() here, as it has already been forgotten, so what is the criterion?
			// We can use the "irrelevant" function, which never forgets (long term memory)
			
			if !Irrelevant(fragment) {
				
				// Connect all the children words to the fragment
				// The ordered combinations are expressed by longer n fragments
				THRESH_ACCEPT++
				
				// fragment(fraglen, number, event)
				key := fmt.Sprintf("F:L%d,N%d,E%d",i,f,index)
				frag := S.CreateFragment(G,key,fragment)

				S.NodeLink(G,event,"CONTAINS",frag,1.0)

				/* children := strings.Split(fragment," ")
				
				for w := range children {
					
					if !Irrelevant(children[w]) {
	
						key := fmt.Sprintf("L_%d__%d_%d_%d",i,f,index,w)
						frag := S.CreateFragment(G,key,children[w])
						S.NodeLink(G,frag,"PART_OF",event,1.0)
					}
					
					//fmt.Printf("->: %d %f (%s) - %s\n",i,Importance(i,fragment),fragment,children[w])
                                */
			}
		}
	}
}

//**************************************************************

func RunningContext() string {

	// Find the top ranked fragments, as they must
	// represent the subject of the narrative somehow
	// don't need to go to MAXCLUSTERS, only 1,2,3

	var hub string = ""

	const min_cluster = 1
	const max_cluster = 3

	for n := min_cluster; n < max_cluster; n++ {

		topics := SkimFrags(n,STM_CLUSTER_RANK[n])

		// Now we want to make a "section hub identifier" from these
		// order them so they form consistently IMPORTANT fragments in spite of context

		sort.Strings(topics)

		top := len(topics)

		// How shall we name hubs? By emotional character plus a hash?

		for topic1 := 0; topic1 < top; topic1++ {

			add := fmt.Sprintf("%d:%s",n,topics[topic1])
			hub = hub + add + ","
		}		
	}

	// Can we measure the lifetime of certain phrases to pick out VERB / NOUN ?

	return hub
}

//**************************************************************

func SkimFrags(n int, source map[string]float64) []string {

	var ranked []float64
	var species = make(map[string]float64)
	var inv = make(map[float64][]string)
	var topics []string

	const skim = 100

	for frag := range source {
		species[frag] = Importance(n,frag)
	}

	for frag := range species {
		inv[species[frag]] = append(inv[species[frag]],frag) // could be multi-valued
		ranked = append(ranked,species[frag])
	}

	sort.Float64s(ranked)

	rlen := len(ranked)
	var start int

	// Pick up top 10 keywords from the important n-fragments
	// This is a sliding window, so it's studying coactivation
	// within a certain radius, not special change or significance
	// But since this only gets called every leg, it can miss things
	// where legs overlap

	if rlen > skim {
		start = rlen - skim
	} else {
		start = 0
	}

	for r := start; r < rlen; r++ {

		key := ranked[r]
		for multi := range inv[key] {
			topics = AppendIdemp(topics,inv[key][multi])
		}
	}

return topics
}

//**************************************************************

func Narrate(text string, rank float64, context string,index int) Narrative {

	var n Narrative

	n.text = text
	n.rank = rank
	n.context = context
	n.index = index

return n
}

//**************************************************************

func TickSample(s int, word string, rrbuffer [MAXCLUSTERS][]string) (float64,[MAXCLUSTERS][]string) {

	var rank float64 = 0

	for i := 2; i < MAXCLUSTERS; i++ {
		
		// Pop from round-robin

		if (len(rrbuffer[i]) > i-1) {
			rrbuffer[i] = rrbuffer[i][1:i]
		}
		
		// Push new to maintain length

		rrbuffer[i] = append(rrbuffer[i],word)

		// Assemble the key, only if complete cluster
		
		if (len(rrbuffer[i]) > i-1) {
			
			var key string
			
			for j := 0; j < i; j++ {
				key = key + rrbuffer[i][j]
				if j < i-1 {
					key = key + " "
				}
			}

			// Add here - listener context flag certain terms of interest (danger signals)

			if BrokenPromise(rrbuffer[i][0],rrbuffer[i][i-1]) {
				continue
			}

			rank += TickUpdate(i,key)

			// Long term memory of fragments

			LTM_CLUSTER_FRAG[i][s] = append(LTM_CLUSTER_FRAG[i][s],key)

			// and keep inverse: which sentence indices (times) phrases appeared?

			LTM_FRAG_HISTORY[i][key] = append(LTM_FRAG_HISTORY[i][key],s)

		}
		
	}

	rank += TickUpdate(1,word)

	LTM_CLUSTER_FRAG[1][s] = append(LTM_CLUSTER_FRAG[1][s],word)
	LTM_FRAG_HISTORY[1][word] = append(LTM_FRAG_HISTORY[1][word],s)

	return rank, rrbuffer
}

//**************************************************************

func SaveContext(ctx *context.Context) {

	name := fmt.Sprintf("/tmp/cellibrium/context")

	f, err := os.Create(name)
	
	if err != nil {
		fmt.Println("Error opening file ",name)
		return
	}

	var context []string
	var hub string

	const min_cluster = 1
	const max_cluster = 6

	for n := min_cluster; n < max_cluster; n++ {

		var ordered []float64
		var inv = make(map[float64]string)

		for key := range STM_CLUSTER_RANK[n] {
			ordered = append(ordered,Importance(n,key))
			inv[Importance(n,key)] = key
		}

		sort.Float64s(ordered)

		var lim = len(ordered)
		var start = lim - n*10

		if start < 0 {
			start = 0
		}

		for key := start; key < lim; key++ {
			s := fmt.Sprintf("%s,%f\n",inv[ordered[key]],ordered[key])
			f.WriteString(s)

			add := fmt.Sprintf("%d:%s",n,inv[ordered[key]])
			hub = hub + add + ","
			context = AppendIdemp(context,inv[ordered[key]])
		}
	}
	
	f.Close()

	/* end := S.ContextHub(ctx,"hub:," + hub)
	fmt.Println("HUB AGAIN")

	for frag := range context {

		end.Fragment(context[frag])
	}*/
}

//**************************************************************

func AppendIdemp(region []string,value string) []string {
	
	for m := range region {
		if value == region[m] {
			return region
		}
	}

	return append(region,value)
}

//**************************************************************

func BrokenPromise(firstword,lastword string) bool {

	// A standalone fragment can't start/end with these words, because they
	// Promise to bind to something else...
	// Rather than looking for semantics, look at spacetime promises only - words that bind strongly
	// to a prior or posterior word.

	if (len(firstword) == 1) || len(lastword) == 1 {
		return true
	}

	var eforbidden = []string{"but", "and", "the", "or", "a", "an", "its", "it's", "their", "your", "my", "of", "as", "are", "is" }

	for s := range eforbidden {
		if lastword == eforbidden[s] {
			return true
		}
	}

	var sforbidden = []string{"and","or","of"}

	for s := range sforbidden {
		if firstword == sforbidden[s] {
			return true
		}
	}

return false 
}

// *****************************************************************

func Irrelevant(word string) bool {

	if len(word) < 3 {
		return true
	}

	var irrel = []string{"hub:", "but", "and", "the", "or", "a", "an", "its", "it's", "their", "your", "my", "of", "if", "we", "you", "i", "there", "as", "in", "then", "that", "with", "to", "is","was", "when", "where", "are", "some", "can", "also", "it", "at", "out", "like", "they", "her", "him", "them", "his", "our", "by", "more", "less", "from", "over", "under", "why", "because", "what", "every", "some", "about", "though", "for", "around", "about", "any", "will","had","all","which" }

	for s := range irrel {
		if irrel[s] == word {
			return true
		}
	}

return false
}



//**************************************************************

func TickUpdate(n int, key string) float64 {

	var rank float64

	if _, ok := STM_CLUSTER_RANK[n][key]; !ok {

		rank = INITIAL_VALUE

	} else {

		rank = REPEATED_HERE_AND_NOW
	}

	STM_CLUSTER_RANK[n][key] = rank

	MemoryDecay(n)

return rank
}

//**************************************************************

func MemoryDecay(n int) {

	const decay_rate = 0.001
	const context_threshold = INITIAL_VALUE

	for k := range STM_CLUSTER_RANK[n] {

		oldv := STM_CLUSTER_RANK[n][k]
		
		// Can't go negative
		
		if oldv > decay_rate {
			
			STM_CLUSTER_RANK[n][k] = oldv - decay_rate

		} else {
			// Help prevent memory blowing up - garbage collection
			delete(STM_CLUSTER_RANK[n],k)
		}
	}
}

//**************************************************************

func MakeDir(pathname string) string {

	prefix := strings.Split(pathname,".")

	subdir := prefix[0]+"_analysis"

	err := os.Mkdir(subdir, 0700)
	
	if err == nil || os.IsExist(err) {
		return subdir 
	} else {
		fmt.Println("Couldn't makedir ",prefix[0])
		os.Exit(1)
	}

return "/tmp"
}

//**************************************************************

func GetSentence(s int) string {

	for t := range SENTENCES {
		if SENTENCES[t].index == s {
			return SENTENCES[t].text
		}
	}
return "<none>"
}

//**************************************************************

func Exists(path string) bool {

    _, err := os.Stat(path)

    if os.IsNotExist(err) { 
	    return false
    }

    return true
}

//**************************************************************

func usage() {
    fmt.Fprintf(os.Stderr, "usage: go run scan_text.go [filelist]\n")
    flag.PrintDefaults()
    os.Exit(2)
}
