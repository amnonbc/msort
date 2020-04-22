package main

import (
	"flag"
	"github.com/amnonbc/msort"
	"log"
	"os"
)

func main() {
	chunkSz := flag.Int("chunkSz", 1000000, "how many numbers to hold in memory simultaneously")
	flag.Parse()
	inFile := flag.Arg(0)
	outFile := flag.Arg(1)
	if inFile == "" || outFile == "" {
		log.Fatal("usage: largesort <infile> <outfile>")
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
