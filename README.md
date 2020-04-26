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
- using 32 bit binary for intermediate files
- using unsafe casts to read and write files directly into []int64 without going through 
binary.LittleEndian
- run merges in parralel
- initially we waited for the initial leafSort pass to complete before starting merging the chunks.
We now start merging as soon as the first two chunks have been written.
- use int32 rather than int64, halving the amnount of disk io.

This speeds up sort time by a factor of 3.

### Before the changes
```
generating 10000000 random numbers
running largesort
real	0m17.807s
user	0m14.183s
sys	0m2.845s
```

### After the changes
```
generating 10000000 random numbers
running largesort
real	0m6.548s
user	0m5.716s
sys	0m1.191s
```

## TODO
We should probably limit the number of active goroutines to the number of cores on the machine.