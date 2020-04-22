
./genrand.awk > testdata/input.txt

go build .

time ./largesort testdata/input.txt testdata/output.txt

cd testdata

time sort -n input.txt -o wanted.txt

diff wanted.txt output.txt && echo SUCCESS

rm -f wanted.txt output.txt input.txt