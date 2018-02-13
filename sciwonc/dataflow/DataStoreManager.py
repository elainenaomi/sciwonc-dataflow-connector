#!/usr/bin/env python

import sys, getopt

def main(argv):
   daxfile = ''
   try:
      opts, args = getopt.getopt(argv,"hd:",["daxfile="])
   except getopt.GetoptError:
      print 'DataStoreManager.py -d <daxfile>'
      sys.exit(2)
   for opt, arg in opts:
     if opt == '-h':
         print 'DataStoreManager.py --dax <dax file>'
         sys.exit()
     elif opt in ("-d", "--dax"):
         daxfile = arg
   print 'Dax file is: ', daxfile

if __name__ == "__main__":
   main(sys.argv[1:])
