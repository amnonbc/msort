# largesort

![Go](https://github.com/amnonbc/msort/workflows/Go/badge.svg)

## Building

`go build ./...`

## Unit tests

`go test ./...`

## Integration test

```
cd cmd/largesort
./test.sh
```

## Improvements
- intermediate files are currently text. Reading and writing will be much 
faster if we use binary files
- Only two files are merged at a time. We could speed this up by
using `runtime.NumCPU()` goroutines to merge simultaneously.
- The first parse spilts the input file into small sorted chunks. At the
moment we wait until this has completed before we start the merge phase.
But we can start merging as soon as there are two chunks available.