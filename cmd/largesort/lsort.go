package main

import (
	"flag"
	"fmt"
	"github.com/amnonbc/msort"
	"log"
	"os"
)

func Usage() {
	fmt.Fprintf(os.Stderr, "usage: %s <infile> <outfile>", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	chunkSz := flag.Int("chunkSz", 1000000, "how many numbers to hold in memory simultaneously")
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
	err = msort.SortFile(outFile, in, *chunkSz)
	if err != nil {
		log.Fatal(err)
	}
}
