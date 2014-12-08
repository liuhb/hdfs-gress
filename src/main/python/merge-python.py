#!/usr/bin/python

#  此脚本可以根据输入的文件名称，确定目标文件名
#
#

import sys, os, re, datetime, shutil

# read the local file from standard input
input_file=sys.stdin.readline()

# 获取上传文件名,可用户生成合并后的文件
filename = os.path.basename(input_file)

#根据服务器时间合并文件
today = datetime.datetime.now()

# http://docs.python.org/library/datetime.html#strftime-strptime-behavior
date_str=today.strftime('%Y%m%d-%H%M%S')

# write it to standard output
print "merged_file_" + date_str;
