#!/usr/bin/awk -f 

BEGIN {
   for (i=0; i < numlines; i++) {
      print int(1000000*rand())
   }
}
