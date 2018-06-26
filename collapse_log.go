package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"os"
	"path"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
)

var start = time.Now()

var filepath = flag.String("f", "", "path to log `file`")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

// COUNTERS
var mux sync.Mutex
var count = make(map[string]int)

func counter(bucket string, value int) {
	mux.Lock()
	count[bucket] += value
	mux.Unlock()
}

// CACHE used for storing hashed value of stack traces
var CACHE = make(map[string]string)

// Init empty Event. Used for aggregating multilines
var event []string

func hashString(s string) string {
	h := xxhash.New64()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum64())
	// return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

func processLine(line string, writer *bufio.Writer) {
	if lineStart(line) {
		// write prior event to file
		flushevent(writer)
		// empty event
		event = event[:0]
		event = append(event, line)
	} else {
		event = append(event, line)
	}
}

// regex match that line starts with `[YYYY.MM.dd`
var linestart, _ = regexp.Compile("^\\[\\d{4}-\\d{2}-\\d{2}")

func lineStart(line string) bool {
	return linestart.Match([]byte(line))
}

var javaclass, _ = regexp.Compile("^(?:[a-zA-Z0-9-]+\\.)+[A-Za-z0-9$]+")

func checkJavaClass(secondline string) bool {
	return javaclass.Match([]byte(secondline))
}

var indentedline, _ = regexp.Compile("^\\s+")

func checkIndentedLine(line string) bool {
	return indentedline.Match([]byte(line))
}

func writeLines(e []string, writer *bufio.Writer) {
	counter("call_write", 1)
	byteArray := []byte(strings.Join(e, ""))

	if _, err := writer.Write(byteArray); err != nil {
		log.Fatal(err)
	}
}

func dstFile(outPath string) (*bufio.Writer, *os.File) {
	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	// writer := bufio.NewWriter(f)
	// 4MiB, stat -f "st_blksize: %k" = st_blksize: 4194304
	// TODO: This could be the wrong optimal size on other drives, should figure
	// how to find this value automatically.
	writer := bufio.NewWriterSize(f, 4194000)
	return writer, f
}

func flushevent(writer *bufio.Writer) {
	// Flush event happens when a new starting line is detected.
	// We can now test if the event is actual multiline, and has what is
	// believed to be a stacktrace.
	// This essentially skips anything without a java class starting the second
	// line. This probably is not correct, and will need adjusted.
	startIndex := 2
	numLines := len(event)
	if numLines >= 3 && checkJavaClass(event[1]) {

		for i, v := range event[2:numLines] {
			if checkIndentedLine(v) {
				startIndex = i + 2
				// fmt.Println(startIndex)
				break
			}
		}
		// create hash of the event, skipping the first line.
		key := hashString(strings.Join(event[startIndex:numLines], ""))
		timestamp := event[0][:25]
		// check if the hash already exists in the CACHE map.
		if val, ok := CACHE[key]; ok {
			// duplicate stacktrace
			counter("matched", 1)
			counter("lines_reduced", numLines-startIndex)
			StackTrace := fmt.Sprintf("StackTrace: %s, %s\n", key, val)
			writeLines([]string{event[0], StackTrace}, writer)
		} else {
			// new stacktrace. store in CACHE & write modified event that includes
			// the hash in the message
			counter("stacktraces", 1)
			CACHE[key] = timestamp
			dup := fmt.Sprintf("StackTrace: %s\n", key)
			writeLines(append([]string{event[0], dup}, event[1:numLines]...), writer)
		}
	} else if numLines > 0 {
		// skip empty event (initial global variable is empty)
		// write the non-stacktrace event to file.
		writeLines(event, writer)
	}
}

func readFile(filepath string, writer *bufio.Writer) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var line string
	for {
		line, err = reader.ReadString('\n')
		processLine(line, writer)
		if err != nil {
			break
		}
	}
	if err != io.EOF {
		fmt.Printf(" > Failed!: %v\n", err)
	}
}

func closeFile(f *os.File) {
	f.Close()
}

func main() {

	// START CPU
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	// END CPU

	// defer close in main, pass buffered write.
	// ugly, need to find a cleaner way.
	var outFilename = path.Base(*filepath)
	var outExtension = path.Ext(outFilename)
	var outFname = outFilename[0 : len(outFilename)-len(outExtension)]
	var outFilepath = path.Join(
		path.Dir(*filepath),
		fmt.Sprintf("%s-reduced%s", outFname, outExtension))

	writer, f := dstFile(outFilepath)
	defer closeFile(f)

	readFile(*filepath, writer)
	log.Printf("writing to file: %s", outFilepath)
	log.Println(count)
	writer.Flush()

	// START MEM
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	// END MEM
	elapsed := time.Since(start)
	log.Printf("go_collapse_log took %s", elapsed)
}
