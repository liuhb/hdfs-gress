#!/usr/bin/python

#
#

import sys, os, re, datetime, shutil

# read the local file from standard input
input_file=sys.stdin.readline()

filename = os.path.basename(input_file)

today = datetime.datetime.now()

# http://docs.python.org/library/datetime.html#strftime-strptime-behavior
date_str=today.strftime('%Y%m%d-%H%M%S')
date_str=today.strftime('%Y%m%d-%H')

# write it to standard output
print "hdfs:/tmp/gress/dest/merged_file_" + date_str;
