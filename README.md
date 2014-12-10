拷贝本地文件到HDFS
=================================================================================

## 动机

很多项目需要把本地文件拷贝到hdfs集群，本程序能自动扫描本地文件并上传到hdfs集群。

##  特性

* 拷贝成功后，可以选择保留或删除文件
* 拷贝文件过程可以支持压缩例如：org.apache.hadoop.io.compress.CompressionCodec
* 拷贝文件可以支持解压，目前只能解压gz文件
* 通过CRC32与文件大小校验，保障传输的准确性。（对于压缩和解压文件，只提供CRC32校验)
* 忽略隐藏文件".*"
* 传输完成后执行定制化脚本，可满足不同项目需要
* 传输前执行定制化脚本。可以灵活满足项目
* 可处理csv文件，自动忽略首行
* 多线程传输
* 传输后，把文件合并到大文件中
* 可设置后台守护运行


## 重要条件

在使用本工具时请注意下面事项

* 文件在移动到原目录时，最好先生成到同一文件系统的其他目录，然后再mv到源目录。或先生成隐藏文件（以 “.”打头的文件）
* 保证文件名称唯一性。
* 所有的路径必须要带上模式，例如 hdfs 文件为 'hdfs:/app' 本地文件为 'file:/tmp'

## 使用

下载源代码

1. 运行 `mvn package`
2. 拷贝 tar 包到目标主机并解压
3. 编辑 `conf/gress-env.sh` 指定hadoop路径
4. 编辑 `conf/gress.conf` 相关参数
5. 运行


### gress.conf 的关键属性

    必须的配置项
1.  "DATASOURCE_NAME", 逻辑数据源名称
2.  "SRC_DIR", 原路径.
3.  "WORK_DIR", 工作路径，copy文件时先copy到work_dir .
4.  "COMPLETE_DIR", copy成功后保存本地文件的副本.
5.  "ERROR_DIR", copy出错后保存到本地的错误文件
6.  "DEST_STAGING_DIR", hdfs上的缓存文件，待copy完成后mv 到目标文件夹
7.  "DEST_DIR", 目标文件夹

    必须的配置项
1. "COMPRESSION_CODEC"  支持文本文件的压缩方式，例如：org.apache.hadoop.io.compress.GzipCodec
2. "CREATE_LZO_INDEX"   对于lzo压缩,是否创建lzo索引
3. "VERIFY"             true 对上传文件进行crc32校验
4. "SCRIPT"             生成目标文件夹的脚本（接受文件名输入,输出一行有效的hdfs全路径)
5. "WORK_SCRIPT"        预处理源文件 （接受文件名输入,输出一行有效的本地文件全路径)
6. "THREADS"            上传的线程数量
7. "CSVHEADER"          true 对于csv文件去掉第一行
8. "UNCOMPRESSTYPE"     源文件的压缩格式，指定压缩格式后，在上传过程中会解压后上传(目前支持gzip压缩格式)
9. "MERGESCRIPT"        生成合并文件的全路径（接受文件名输入,输出一行有效的hdfs全路径)
10. "MERGE_DIR"         指定合并后文件的全路径
11. "DAEMON"            true 表示守护运行

    注意：SCRIPT, MERGESCRIPT 和MERGE_DIR 互斥的，三者只能选其一
          UNCOMPRESSTYPE 与 COMPRESSION_CODEC 互斥的，二者只能选其一
          源文件是压缩文件，如果是gzip目前支持合并，且必须配置UNCOMPRESSTYPE选项。其他压缩格式不支持



### 实例 1

copy  `/tmp/gress/in` 下的文件到 HDFS 目录 `/incoming/`.

<pre><code>shell$ cat conf/examples/basic.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/gress/in
WORK_DIR = file:/tmp/gress/work
COMPLETE_DIR = file:/tmp/gress/complete
ERROR_DIR = file:/tmp/gress/error
DEST_STAGING_DIR = hdfs:/incoming/stage
DEST_DIR = hdfs:/incoming
DAEMON = true
</code></pre>

在控制台运行gress.sh

<pre><code>shell$ bin/gress.sh \
  --config-file /path/to/gress/conf/examples/basic.conf
</code></pre>

在另外一个控制台运行

<pre><code>shell$ echo "test" > /tmp/gress/in/test.txt
</code></pre>


### 实例2

压缩并上传数据，其中压缩COMPRESSION_CODEC 支持hadoop自带的所有格式

<pre><code>shell$ cat conf/examples/lzop-verify.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/gress/in
WORK_DIR = file:/tmp/gress/work
COMPLETE_DIR = file:/tmp/gress/complete
ERROR_DIR = file:/tmp/gress/error
DEST_STAGING_DIR = hdfs:/incoming/stage
DEST_DIR = hdfs:/incoming
COMPRESSION_CODEC = com.hadoop.compression.lzo.LzopCodec
CREATE_LZO_INDEX = true
VERIFY = true
</code></pre>

### 实例3

解压并上传数据，目前只支持gz解压

<pre><code>shell$ cat conf/examples/lzop-verify.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/gress/in
WORK_DIR = file:/tmp/gress/work
COMPLETE_DIR = file:/tmp/gress/complete
ERROR_DIR = file:/tmp/gress/error
DEST_STAGING_DIR = hdfs:/incoming/stage
DEST_DIR = hdfs:/incoming
UNCOMPRESSTYPE= gzip
VERIFY = true
</code></pre>



### 实例4
利用脚本上传到目标的不同目录

<pre><code>shell$ cat bin/sample-python.py
#!/usr/bin/python

import sys, os, re

# read the local file from standard input
input_file=sys.stdin.readline()

# extract the filename from the file
filename = os.path.basename(input_file)

# extract the date from the filename
match=re.search(r'([0-9]{4})([0-9]{2})([0-9]{2})', filename)

year=match.group(1)
mon=match.group(2)
day=match.group(3)

# construct our destination HDFS file
hdfs_dest="hdfs:/data/%s/%s/%s/%s" % (year, mon, day, filename)

# write it to standard output
print hdfs_dest,
</code></pre>

### 实例5 定义SCRIPT脚本

<pre><code>shell$ cat conf/examples/dynamic-dest.conf
DATASOURCE_NAME = test
SRC_DIR = file:/tmp/gress/in
WORK_DIR = file:/tmp/gress/work
COMPLETE_DIR = file:/tmp/gress/complete
ERROR_DIR = file:/tmp/gress/error
DEST_STAGING_DIR = hdfs:/incoming/stage
SCRIPT = /path/to/gress/bin/sample-python.py
</code></pre>

### 实例6 合并文件上传
<pre><code>
DATASOURCE_NAME = test
SRC_DIR = file:/app/data/gress/in
WORK_DIR = file:/app/data/gress/work
COMPLETE_DIR = file:/app/data/gress/complete
REMOVE_AFTER_COPY = false
ERROR_DIR = file:/app/data/gress/error
DEST_STAGING_DIR = hdfs:/tmp/gress/stage
#DEST_DIR = hdfs:/tmp/gress/dest
#COMPRESSION_CODEC = org.apache.hadoop.io.compress.GzipCodec
#CREATE_LZO_INDEX = true
VERIFY = true
# SCRIPT = /tmp/sample-python.py
# WORK_SCRIPT = /tmp/sample-stage-python.py
THREADS = 5
#CSVHEADER = true
#MERGESCRIPT = /tmp/merge-python.py
#UNCOMPRESSTYPE = gzip
MERGESCRIPT = /path/to/gress/bin/merge-python.py
DAEMON = true
</code></pre>
程序使用

<pre><code>shell$ touch /tmp/gress/in/apache-20110202.log

shell$ bin/gress.sh \
  --config-file /path/to/gress/conf/examples/basic.conf

Launching script '/tmp/hdfs-file-gress/src/main/python/sample-python.py' and piping the following to stdin 'file:/tmp/gress/work/apache-20110202.log'
Copying source file 'file:/tmp/gress/work/apache-20110202.log' to staging destination 'hdfs:/incoming/stage/675861557'
Attempting creation of target directory: hdfs:/data/2011/02/02
Local file size = 0, HDFS file size = 0
Moving staging file 'hdfs:/incoming/stage/675861557' to destination 'hdfs:/data/2011/02/02/apache-20110202.log'
File copy successful, moving source file:/tmp/gress/work/apache-20110202.log to completed file file:/tmp/gress/complete/apache-20110202.log
</code></pre>

