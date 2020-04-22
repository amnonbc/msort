#!/usr/bin/awk -f 

BEGIN {
   for (i=0; i < 1000000; i++) {
      print int(1000000*rand())
   }
}
