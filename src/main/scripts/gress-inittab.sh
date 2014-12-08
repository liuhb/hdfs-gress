#! /usr/bin/env bash
##########################################################################
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
##########################################################################
#
# slurper.sh:  Copy files between local disk and HDFS.
#
# This script is customized to work nice with inittab respawn.
#
# Be warned that if you launch this in a console window and Ctrl+C
# the process, the child Java process will still be running in the
# background.
#
##########################################################################

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`
SCRIPT=`basename ${PRG}`

cd ${BASEDIR}

. ${BASEDIR}/bin/base

date=`date +"%Y%m%d-%H%M%S"`
outfile=$BASEDIR/logs/gress-$date.out

nohup ${HADOOP_BIN} com.sponge.srd.hdfsgress.Gress "$@" --log4j-file ${BASEDIR}/conf/daemon/log4j.properties &> $outfile < /dev/null &
PID="$!"
trap "kill $PID" SIGTERM
wait
