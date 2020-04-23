
./genrand.awk -v numlines=10000000 > testdata/input.txt

go build .

echo running largesort
time ./largesort testdata/input.txt testdata/output.txt

cd testdata

echo running unix sort
time sort -n input.txt -o wanted.txt

diff wanted.txt output.txt && echo SUCCESS

#rm -f wanted.txt output.txt input.txt