package main

import (
	"flag"
	"fmt"
	"github.com/amnonbc/msort"
	"log"
	"os"
	"runtime"
)

func Usage() {
	fmt.Fprintf(os.Stderr, "usage: %s <infile> <outfile>", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	chunkSz := flag.Int("chunkSz", 1000000, "how many numbers to hold in memory simultaneously")
	verbose := flag.Bool("verbose", false, "print runtime metrics")
	flag.Parse()
	inFile := flag.Arg(0)
	outFile := flag.Arg(1)
	if inFile == "" || outFile == "" {
		Usage()
		return
	}
	in, err := os.Open(inFile)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	err = msort.SortFile(out, in, *chunkSz)
	if err != nil {
		log.Fatal(err)
	}
	if *verbose {
		logFileStats(inFile)
		logMemStats()
		log.Println(runtime.NumCPU(), "cores")
	}

}

const M = 1024 * 1024

func logFileStats(fn string) {
	st, err := os.Stat(fn)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("data read:", st.Size()/M, "Mb")
}

func logMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Println("Total memory allocated:", m.TotalAlloc/M, "Mb")
}
