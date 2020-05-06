NUM=10000000
echo generating $NUM random numbers
test -d testdata || mkdir testdata
./genrand.awk -v numlines=$NUM > testdata/input.txt

go build .

echo running largesort
time ./largesort -verbose testdata/input.txt testdata/output.txt

cd testdata

echo running unix sort
time sort -n input.txt -o wanted.txt

echo comparing our output with unix sort
diff wanted.txt output.txt && echo SUCCESS
rm -f wanted.txt output.txt input.txt