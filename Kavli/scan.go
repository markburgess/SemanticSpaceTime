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
	contextid string
	context []string
	index int
}

var WORDCOUNT int = 0
var LEGCOUNT int = 0

var KEPT int = 0
var SKIPPED int = 0

var TOTAL_PARAGRAPHS int = 0
var ALL_SENTENCE_INDEX int = 0

var SELECTED_SENTENCES []Narrative

var THRESH_ACCEPT float64 = 0
var TOTAL_THRESH float64 = 0
var MAX_IMPORTANCE float64

// ************** SOME INTRINSIC SPACETIME SCALES ****************************

const MAXCLUSTERS = 7
const LEG_WINDOW = 100

var ATTENTION_LEVEL float64 = 0.6
var SENTENCE_THRESH float64 = 10

const REPEATED_HERE_AND_NOW  = 1.0 // initial primer
const INITIAL_VALUE = 0.5

const MEANING_THRESH = 20      // reduce this if too few samples
const FORGET_FRACTION = 0.001  // this amount per sentence ~ forget over 1000 words

// ****************************************************************************
// The ranking vectors for structural objects in a narrative
// LHS = type (semantic, metric) and RHS = importance / relative meaning
// ****************************************************************************

// n-phrase clusters by sentence are semantic units (no relevant order) - these are memory
// implicated in selection at the "smart sensor" level, i.e. innate adaptation
// about what is retained from the incomning `signal'

var LTM_NGRAMS_IN_SENTENCE [MAXCLUSTERS]map[int][]string

// inverse: in which sentences did the ngrams appear? Sequence of integer times by ngram
var LTM_EVERY_NGRAM_OCCURRENCE [MAXCLUSTERS]map[string][]int

var HISTO_AUTO_CORRE_NGRAM [MAXCLUSTERS]map[int]int  // [sentence_distance]count

// Short term memory is used to cache the ngram scores
var STM_NGRAM_RANK [MAXCLUSTERS]map[string]float64
var LTM_NGRAM_RANK [MAXCLUSTERS]map[string]float64

var G S.Analytics

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

	for i := 1; i < MAXCLUSTERS; i++ {

		STM_NGRAM_RANK[i] = make(map[string]float64)
		LTM_NGRAM_RANK[i] = make(map[string]float64)
		LTM_NGRAMS_IN_SENTENCE[i] = make(map[int][]string)
		LTM_EVERY_NGRAM_OCCURRENCE[i] = make(map[string][]int)
	} 
	
	// ***********************************************************

	S.InitializeSmartSpaceTime()

	var dbname string = "SemanticSpacetime"
	var url string = "http://localhost:8529"
	var user string = "root"
	var pwd string = "mark"

	// ***********************************************************

	G = S.OpenAnalytics(dbname,url,user,pwd)

	for i := range args {

		if strings.HasSuffix(args[i],".dat") {

			ParseDocument(args[i])  // Once for whole thing, reset and compare to realtime

			SearchInvariants(G)

			FilterAndAnnotateSelectedEvents(args[i])

		}
	}

	SaveContext()

	fmt.Println("\nKept = ",KEPT,"of total ",ALL_SENTENCE_INDEX,"efficiency = ",100*float64(ALL_SENTENCE_INDEX)/float64(KEPT),"%")
	fmt.Println("\nAccepted",THRESH_ACCEPT/TOTAL_THRESH*100,"% into hubs")

	fmt.Println("Average sentence length =")
}

//**************************************************************
// Start scanning docs
//**************************************************************

func ParseDocument(filename string) {

	/// Use the filename as context

	start := strings.ReplaceAll(path.Base(filename),"/",":")

	S.NextDataEvent(&G,start,start)

	TOTAL_PARAGRAPHS = len(Scanfile(filename))
}

//**************************************************************

func Scanfile(filename string) []string {

	// split each file into paragraphs. These paragraphs aren't significant for
	// processing, because styles use paragraphs in diff ways, so we use "legs" instead.

	proto_paragraphs := CleanFile(string(filename))
	
	// split each paragraph chunk into sentences

	for para := range proto_paragraphs {

		// emotion, take a breath

		ParseChunk(para, proto_paragraphs[para])
	}

return proto_paragraphs
}

//**************************************************************

func CleanFile(filename string) []string {

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

	m3 := regexp.MustCompile("[–{&}“#%^+_#”=$’~‘/()<>\"&]*") 
	stripped3 := m3.ReplaceAllString(stripped2,"") 

	// Treat ? and ! as end of sentence
	m3a := regexp.MustCompile("[?!]+") 
	stripped3a := m3a.ReplaceAllString(stripped3,".") 

	// Strip digits, this is probably wrong in general
	m4 := regexp.MustCompile("[0-9]*")
	stripped4 := m4.ReplaceAllString(stripped3a,"")

	m5 := regexp.MustCompile("[^ a-zA-Z.,]*")
	stripped5 := m5.ReplaceAllString(stripped4,"")

	// Close multiple redundant spaces
	m6 := regexp.MustCompile("[ ]+")
	stripped6 := m6.ReplaceAllString(stripped5," ")

	// Now we should have a standard paragraph format but
        // this is format dependent, so add a maximum length limit.
	
	proto_paragraphs := strings.Split(string(stripped6),"\n\n")

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

func ParseChunk(p int, paragraph string){

	var sentences []string

	// Coordinatize the non-trivial sentences in terms of their ngrams

	if len(paragraph) == 0 {
		return
	}

	sentences = SplitIntoSentences(paragraph)

	for s_idx := range sentences {
		
		meaning := FractionateThenRankSentence(ALL_SENTENCE_INDEX,sentences[s_idx])

		ctxid,context := RunningFeelingAndSTMContext()
		
		if SentenceMeetsAttentionThreshold(meaning,sentences[s_idx]) {

			n := NarrationMarker(sentences[s_idx] + ".", meaning, ctxid,context,ALL_SENTENCE_INDEX)
			
			// The context hub name is stored with the selected sentence
			
			SELECTED_SENTENCES = append(SELECTED_SENTENCES,n)
		}
		
		ALL_SENTENCE_INDEX++
	}
}

//**************************************************************

func SentenceMeetsAttentionThreshold(meaning float64, sentence string) bool {

	const alert = 1.0
	const awake = 0.5
	const attention_deficit = 0.1
	const sentence_width = 7
	const response = 0.6
	const calm = 0.9

	// If sudden change in sentence length, be alert

	slen := float64(len(sentence))

	if (slen > SENTENCE_THRESH + sentence_width) {

		ATTENTION_LEVEL = alert
		SENTENCE_THRESH = response * slen + (1-response) * SENTENCE_THRESH
	}

	if (slen < SENTENCE_THRESH - sentence_width) {

		ATTENTION_LEVEL = alert
		SENTENCE_THRESH = response * slen + (1-response) * SENTENCE_THRESH
	}

	if (meaning > MEANING_THRESH) && (ATTENTION_LEVEL > awake) {

		KEPT++

		if ATTENTION_LEVEL > attention_deficit {

			ATTENTION_LEVEL -= attention_deficit
		}

		//fmt.Println("\nKeeping: ", sentence)

		return true

	} else {
		
		//fmt.Println("\nSkipping: ", sentence)

		SKIPPED++
		return false
	}
}

//**************************************************************

func SplitIntoSentences(para string) []string {

	sentences := strings.Split(para,".")

	var cleaned []string

	for sen := range sentences{

		// Split first by punctuation marks, because phrases don't cross these boundaries

		f := func(c rune) bool {       // Inline function

			// Something serious going on, so perk up

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

func FractionateThenRankSentence(s_idx int, sentence string) float64 {

	var rrbuffer [MAXCLUSTERS][]string
	var sentence_meaning_rank float64 = 0
	var rank float64

	// For one sentence, break it up into codons and sum their importances

	no_dot := strings.ReplaceAll(sentence,"."," ")
	no_comma := strings.ReplaceAll(no_dot,","," ")
	no_dash := strings.ReplaceAll(no_comma,"—"," ")
	clean_sentence := strings.Split(no_dash," ")

	for word := range clean_sentence {

		// This will be too strong in general - ligatures in xhtml etc

		m := regexp.MustCompile("[^-a-zA-Z0-9]*") 
		cleanjunk := m.ReplaceAllString(clean_sentence[word],"") 
		cleanword := strings.ToLower(cleanjunk)

		if len(cleanword) == 0 {
			continue
		}

		// Shift all the rolling longitudinal Ngram rr-buffers by one word
		rank, rrbuffer = NextWordAndUpdateLTMNgrams(s_idx,cleanword, rrbuffer)
		sentence_meaning_rank += rank
	}

return sentence_meaning_rank
}

//**************************************************************

func SearchInvariants(g S.Analytics) {

	fmt.Println("----- LONGITUDINAL INVARIANTS (THEMES) ----------")

	for n := 1; n < MAXCLUSTERS; n++ {

		var last,delta int

		HISTO_AUTO_CORRE_NGRAM[n] = make(map[int]int,0)

		// Search through all sentence ngrams and measure distance between repeated
		// try to indentify any scales that emerge

		for ngram := range LTM_EVERY_NGRAM_OCCURRENCE[n] {

			if (InsignificantPadding(ngram)) {
				continue
			}

			occurrences := len(LTM_EVERY_NGRAM_OCCURRENCE[n][ngram])

			// occurrences per unit length, per leg - constant or variable?

			if occurrences > (MAXCLUSTERS - n) {

				fmt.Println("Theme long invariant",ngram,occurrences)

			} else {
				continue
			}

			last = 0

			for location := 0; location < occurrences; location++ {

				// Foreach occurrence, check proximity to others
				// This is about seeing if an ngram is a recurring input in the stream.
				// Does the subject recur several times over some scale? The scale may be
				// logarithmic like n / log (o1-o2) for occurrence separation
				// Radius = 100 sentences, how many occurrences of this ngram close together?
				
				// Does meaning have an intrinsic radius. It doesn't make sense that it
				// depends on the length of the document. How could we measure this?	
				
				// two one relative to first occurrence (absolulte range), one to last occurrence??
				// only the last is invariant on the scale of a story
				
				delta = LTM_EVERY_NGRAM_OCCURRENCE[n][ngram][location] - last			
				last = LTM_EVERY_NGRAM_OCCURRENCE[n][ngram][location]

				//fmt.Println("DELTA",delta,delta/10*10)
				HISTO_AUTO_CORRE_NGRAM[n][delta/LEG_WINDOW*LEG_WINDOW]++

			}
		}

		PlotClusteringGraph(n)
	}
	
	fmt.Println("-------------")
}

//**************************************************************

func PlotClusteringGraph(n int) {

	name := fmt.Sprintf("/tmp/cellibrium/clusters_%d_grams",n)

	f, err := os.Create(name)
	
	if err != nil {
		fmt.Println("Error opening file ",name)
		return
	}

	var keys []int

	for v := range HISTO_AUTO_CORRE_NGRAM[n] {
		keys = append(keys,v)
	}

	sort.Ints(keys)

	for delta := range keys {
		s := fmt.Sprintf("%d %d\n",keys[delta],HISTO_AUTO_CORRE_NGRAM[n][keys[delta]])
		f.WriteString(s)
	}

	f.Close()
}

// *****************************************************************

func FilterAndAnnotateSelectedEvents(filename string) {

	// The importances have now all been measured in realtime, but we review them now...posthoc
	// Now go through the history map chronologically, by sentence only reset the narrative  
        // `leg' counter when it fills up to measure story progress. 
	// This determines the sampling density of "important sentences" - pick a few from each leg

	var steps,leg int

	const leg_reset = LEG_WINDOW // measured in sentences

	// Sentences to summarize per leg of the story journey

	steps = 0

	var imp_l float64 = 0
	var imp_leg []float64
	
	// First, coarse grain the narrative into `legs', i.e. standardized "paragraphs" by meter not syntax

	for s := range SELECTED_SENTENCES {

		// Sum the importances of each selected sentence

		imp_l += SELECTED_SENTENCES[s].rank

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

	for s := range SELECTED_SENTENCES {

		// Keep the latest running context summary hub, as we go through the sentences

		max_rank[leg][SELECTED_SENTENCES[s].rank] = s

		if steps > leg_reset {

			imp_l = imp_leg[leg]

			AnnotateLeg(filename, leg, max_rank[leg], imp_l, max_leg)

			steps = 0
			leg++

			max_rank[leg] = make(map[float64]int)
		}

		steps++
	}

	// Don't forget the final remainder (catch leg++)

	imp_l = imp_leg[leg]
	
	AnnotateLeg(filename, leg, max_rank[leg], imp_l, max_leg)
}

//**************************************************************
// TOOLKITS
//**************************************************************

func Intentionality(n int, s string) float64 {

	// Emotional bias to be added ?

	if _, ok := STM_NGRAM_RANK[n][s]; !ok {

		return 0
	}

	// Things that are repeated too often are not important
	// but length indicates purposeful intent

	meaning := float64(len(s)) / (0.5 + STM_NGRAM_RANK[n][s] )

	if meaning > MAX_IMPORTANCE {
		MAX_IMPORTANCE = meaning
	}

return meaning
}

//**************************************************************

func AnnotateLeg(filename string, leg int, random map[float64]int, leg_imp,max float64) {

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

		fmt.Printf("\nEVENT[Leg %d selects %d]: %s\n",leg,ordered[s],SELECTED_SENTENCES[ordered[s]].text)

		AnnotateSentence(filename,s,SELECTED_SENTENCES[ordered[s]].text)
	}
}

//**************************************************************

func AnnotateSentence(filename string, s_number int,sentence string) {

	// We use the unadulterated sentence itself as an episodic event
	// This acts as an impromptu hub

	key := S.KeyName(sentence) //fmt.Sprintf("%s_Sentence_%d",prefix,s_number)

	event := S.NextDataEvent(&G, key, sentence)

	// Keep the 3-fragments and above that are important enough to pass threshold
	// Then hierarchically break them into words that are important enough.

	hub := S.KeyName(SELECTED_SENTENCES[s_number].contextid)
	hubnode := S.CreateHub(G,hub,SELECTED_SENTENCES[s_number].contextid,1)

	S.CreateLink(G,hubnode,"CONTAINS",event,1)

	for frag := range SELECTED_SENTENCES[s_number].context {

		fragkey := S.KeyName(SELECTED_SENTENCES[s_number].context[frag])
		ngram := S.CreateFragment(G,fragkey,SELECTED_SENTENCES[s_number].context[frag],1)
		S.CreateLink(G,hubnode,"DEPENDS",ngram,1)
	}

	// So we have a hierarchy: context_hub - sentence - phrases - significant words

	const min_cluster = 3
	const max_cluster = 6
	const incr = 2

	for i := min_cluster; i < max_cluster; i += incr {

		// LTM_NGRAMS_IN_SENTENCE is the ngrams from sentence number index - how is this different from context?
		// context may contain additional info about environment, and is quality ranked

		for f := range LTM_NGRAMS_IN_SENTENCE[i][s_number] {
			
			fragment := LTM_NGRAMS_IN_SENTENCE[i][s_number][f]
			
			TOTAL_THRESH++
			
			// We can't use Intentionality() here, as it has already been forgotten, so what is the criterion?
			// We can use the "irrelevant" function, which never forgets (long term memory)
			
			if !InsignificantPadding(fragment) {
				
				// Connect all the children words to the fragment
				// The ordered combinations are expressed by longer n fragments
				THRESH_ACCEPT++
				
				key := S.KeyName(fragment) // fmt.Sprintf("F:L%d,N%d,E%d",i,f,s_number)
				frag := S.CreateFragment(G,key,fragment,1.0)

				// Sentence contains fragment
				S.CreateLink(G,event,"CONTAINS",frag,1.0)

			}
		}
	}
}

//**************************************************************

func NextWordAndUpdateLTMNgrams(s_idx int, word string, rrbuffer [MAXCLUSTERS][]string) (float64,[MAXCLUSTERS][]string) {

	var rank float64 = 0

	for n := 2; n < MAXCLUSTERS; n++ {
		
		// Pop from round-robin

		if (len(rrbuffer[n]) > n-1) {
			rrbuffer[n] = rrbuffer[n][1:n]
		}
		
		// Push new to maintain length

		rrbuffer[n] = append(rrbuffer[n],word)

		// Assemble the key, only if complete cluster
		
		if (len(rrbuffer[n]) > n-1) {
			
			var key string
			
			for j := 0; j < n; j++ {
				key = key + rrbuffer[n][j]
				if j < n-1 {
					key = key + " "
				}
			}

			if ExcludedByBindings(rrbuffer[n][0],rrbuffer[n][n-1]) {

				continue
			}

			rank += MemoryUpdateNgram(n,key)

			LTM_NGRAMS_IN_SENTENCE[n][s_idx] = append(LTM_NGRAMS_IN_SENTENCE[n][s_idx],key)
			LTM_EVERY_NGRAM_OCCURRENCE[n][key] = append(LTM_EVERY_NGRAM_OCCURRENCE[n][key],s_idx)

		}
	}

	rank += MemoryUpdateNgram(1,word)

	LTM_NGRAMS_IN_SENTENCE[1][s_idx] = append(LTM_NGRAMS_IN_SENTENCE[1][s_idx],word)
	LTM_EVERY_NGRAM_OCCURRENCE[1][word] = append(LTM_EVERY_NGRAM_OCCURRENCE[1][word],s_idx)

	return rank, rrbuffer
}

//**************************************************************
// MISC
//**************************************************************

func NarrationMarker(text string, rank float64, contextname string, context []string, index int) Narrative {

	var n Narrative

	n.text = text
	n.rank = rank
	n.contextid = contextname
	n.context = context
	n.index = index

return n
}

//**************************************************************

func RunningFeelingAndSTMContext() (string,[]string) {

	// Find the top ranked fragments, as they must
	// represent the subject of the narrative somehow
	// don't need to go to MAXCLUSTERS, only 1,2,3

	var hub string = ""
	var topics []string

	const min_cluster = 1
	const max_cluster = 3

	for n := min_cluster; n < max_cluster; n++ {

		topics = SkimFrags(n,STM_NGRAM_RANK[n])

		// Now we want to make a "section hub identifier" from these
		// order them so they form consistently IMPORTANT fragments in spite of context

		sort.Strings(topics)

		top := len(topics)

		// How shall we name hubs? By emotional character plus a hash?

		for topic1 := 0; topic1 < top; topic1++ {

			hub = hub + topics[topic1] + ","
		}		
	}

	return hub, topics
}

//**************************************************************

func SkimFrags(n int, source map[string]float64) []string {

	var ranked []float64
	var species = make(map[string]float64)
	var inv = make(map[float64][]string)
	var topics []string

	const skim = 100

	for frag := range source {
		species[frag] = Intentionality(n,frag)
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

	//fmt.Println("CONTEXT",topics)

return topics
}

//**************************************************************

func SaveContext() {

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

		for key := range STM_NGRAM_RANK[n] {
			ordered = append(ordered,Intentionality(n,key))
			inv[Intentionality(n,key)] = key
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

func ExcludedByBindings(firstword,lastword string) bool {

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

func InsignificantPadding(word string) bool {

	// This is a shorthand for the most common words and phrases, which may be learned by scanning many docs
	// Earlier, we learned these too, now just cache them

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

func MemoryUpdateNgram(n int, key string) float64 {

	// Decay rate approximately once per sentence, assuming no repeated ngrams

	var rank float64

	if _, ok := STM_NGRAM_RANK[n][key]; !ok {

		rank = INITIAL_VALUE

	} else {

		rank = REPEATED_HERE_AND_NOW
	}

	STM_NGRAM_RANK[n][key] = rank
	LTM_NGRAM_RANK[n][key]++

	// Diffuse ALL concepts - should probably be handled by "dream" phase

	MemoryDecay(n)

return rank
}

//**************************************************************

func MemoryDecay(n int) {

	const decay_rate = FORGET_FRACTION // probability linear decay rate per word
	const context_threshold = INITIAL_VALUE

	for k := range STM_NGRAM_RANK[n] {

		oldv := STM_NGRAM_RANK[n][k]
		
		// Can't go negative
		
		if oldv > decay_rate {
			
			STM_NGRAM_RANK[n][k] = oldv - decay_rate

		} else {
			// Help prevent memory blowing up - garbage collection, forget forever
			delete(STM_NGRAM_RANK[n],k)
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

	for t := range SELECTED_SENTENCES {

		if SELECTED_SENTENCES[t].index == s {
			return SELECTED_SENTENCES[t].text
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
