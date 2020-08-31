#########################################################################
#  brief: common
#version: 2017-01-01     create            lhzd863
#########################################################################

source ${BATCH_HOME}/profile/env.profile
source ${BATCH_HOME}/lib/batch-common-util.sh
#

#variable
cal_sys=""
cal_job=""
#yyyyMMdd
cal_date=""
cal_hour=""
cal_minute=""
cal_second=""

#
cal_tbname=""
#yyyy-MM-dd
cal_date_0=""
#HHmmss
cal_time=""
#HH:mm:ss
cal_time_0=""
#cal_date-1
#yyyyMMdd
cal_date1=""
#yyyy-MM-dd
cal_date_1=""
#cal_date-2
#yyyyMMdd
cal_date2=""
#yyyy-MM-dd
cal_date_2=""

#
cal_platform=""
#
batch_band_host=""
batch_db_host=""
batch_db_usr=""
batch_db_pwd=""
batch_db_usrcnt=0
batch_qry_band=""

#########################################################################
#  brief: submit HiveSQL cmd,input sql statement
#version: 2017-01-01     create            lhzd863
#########################################################################
function   hivecmd()
{
   local cmdsql="$1"
   
   local sql="
     set mapreduce.job.reduce.slowstart.completedmaps=0.9;                                                                                    
     set mapreduce.job.queuename=root.open;                                                                                                   
     set hive.exec.dynamic.partition.mode=nonstrict;                                                                                          
     set mapred.job.name=${cal_job}_${cal_date}_${cal_time};         
      $cmdsql
   "
   seq1=`date +%Y%m%d%H%M%S`
   seq2=$RANDOM
   spoolscriptname="${BATCH_LOCAL_FILESPOOL}/h-${cal_job}-${seq1}-${seq2}.sql"
   echo "${sql}">${spoolscriptname}
    
   hive -v -f ${spoolscriptname}
   local ret=$?
   rm -f ${spoolscriptname}
   if [ $ret -ne 0 ]; then
      return $ret
   fi
   return 0
}
#########################################################################
#  brief: submit HiveSQL cmd,input sql statement,silent
#version: 2017-01-01     create            lhzd863
#########################################################################
function   hivecmds()
{
   local cmdsql="$1"
   
   local sql="
     set mapreduce.job.reduce.slowstart.completedmaps=0.9;                                                                                    
     set mapreduce.job.queuename=root.open;                                                                                                   
     set hive.exec.dynamic.partition.mode=nonstrict;                                                                                          
     set mapred.job.name=${cal_job}_${cal_date}_${cal_time};
     $cmdsql
   "
   hive -e "$sql"
   local ret=$?
   if [ $ret -ne 0 ]; then
      return $ret
   fi
   return 0
}
#########################################################################
#  brief: submit MySQL cmd,input sql statement
#version: 2017-01-01     create            lhzd863
#########################################################################
function mysqlcmd()
{
   local cmdsql="$1"

   local sql="
     $cmdsql
   "
   #
   mysql  -u${batch_db_usr} -h${batch_db_host} -p${batch_db_pwd} -N --local-infile=1 -v  -e "$sql"
   local ret=$?
   if [ $ret -ne 0 ]; then
      return $ret
   fi
   return 0
}

#########################################################################
#  brief: submit MySQL cmd,input sql statement,silent
#version: 2017-01-01     create            lhzd863
#########################################################################
function mysqlcmds()
{
   local cmdsql="$1"
   
   local sql="
     $cmdsql
   "
   #
   mysql  -u${batch_db_usr} -h${batch_db_host} -p${batch_db_pwd} -N --local-infile=1  -e "$sql"
   local ret=$?
   if [ $ret -ne 0 ]; then
      return $ret
   fi
   return 0
}
#########################################################################
#  brief: submit Postgresql cmd,input sql statement
#version: 2017-01-01     create            lhzd863
#########################################################################
function psqlcmd()
{
   cmdsql="$1"
   local host=""

   local sql="
     $cmdsql
   "
   echo "$sql"
   #
   psql "host=127.0.0.1 port=5432 user=test password=test dbname=test" --command "$sql"
   ret=$?
   if [ $ret -ne 0 ]; then
      return $ret
   fi
   return 0
}

#########################################################################
#  brief: failure quit
#version: 2017-01-01     create            lhzd863
#########################################################################
function errquit()
{
  local errcd="$1"
  local errdescribe="$2"
  
  if [ $errcd -ne 0 ];then
     echo "  *** Failure : $errcd ${errdescribe}"
     exit $errcd
  fi
}
#########################################################################
#  brief: success quit
#version: 2017-01-01     create            lhzd863
#########################################################################
function succquit()
{
  local errcd="$1"
  local errdescribe="$2"
  
  if [ $errcd -ne 0 ];then
     echo "  *** Failure :$errcd ${errdescribe}"
     exit 0
  fi
}

#########################################################################
#  brief: parse user name and password
#       : base64 -i encrypt
#       : base64 -d decrypt
#version: 2017-01-01     create            lhzd863
#########################################################################
function logininfo()
{
   #
   while read line
   do
        #blank string
        if [ ${#line} -lt 1 ];then
           continue
        fi
        #begin
        if [ `echo "${line}"|grep "^#"|wc -l|sed 's/ //g'` -gt 0 ];then
          continue
        fi
        if [ `echo "${line}"|awk -F '|' '{print NF}'|sed 's/ //g'` -ne 5 ];then
          echo "config columns not equal 5,ingore this line"
          continue
        fi
        #
        local tmp_host=`echo "$line"|awk -F '|' '{print $1}'`
        local tmp_sys=`echo "$line"|awk -F '|' '{print $2}'`
        local tmp_job=`echo "$line"|awk -F '|' '{print $3}'`
        local tmp_usr=`echo "$line"|awk -F '|' '{print $4}'`
        local tmp_usrcnt=`echo "$line"|awk -F '|' '{print $5}'`
        #
        if [ "${cal_job}" == "${tmp_job}" ];then
           batch_db_host="${tmp_host}"
           batch_db_usr="${tmp_usr}"
           batch_db_usrcnt="${tmp_usrcnt}"
           break
        fi
         
        if [ "${cal_sys}" == "${tmp_sys}" ]&&[ ${#tmp_job} -eq 0 ]&&([ ${#batch_band_host} -eq 0 ]|| [ "${batch_band_host}" == "${tmp_host}" ]);then
           batch_db_host="${tmp_host}"
           batch_db_usr="${tmp_usr}"
           batch_db_usrcnt="${tmp_usrcnt}"
        fi
   done  < "${BATCH_HOME}/etc/login_usr.conf"
   
   #
   if [ $batch_db_usrcnt -eq 0 ];then
      echo "etc/login_use.conf record $cal_sys or $cal_job usr cnt is 0 or no"
      return
   fi
   
   #
   while read line
   do
        #blank string
        if [ ${#line} -lt 1 ];then
           continue
        fi
        #begin
        if [ `echo "${line}"|grep "^#"|wc -l|sed 's/ //g'` -gt 0 ];then
          continue
        fi
        #
        tmp_host=`echo "$line"|awk -F '|' '{print $1}'`
        tmp_usr=`echo "$line"|awk -F '|' '{print $2}'`
        tmp_pwd=`echo "$line"|awk -F '|' '{print $3}'`
        #
        if [ "${tmp_host}" == "${batch_db_host}" ]&&[ "$batch_db_usr" == "$tmp_usr" ];then
           batch_db_pwd=`echo "$tmp_pwd"|base64 -d`
           break
        fi
   done  < "${BATCH_HOME}/etc/login_passwd.conf"
   
   #
   if [ $batch_db_usrcnt -ne 1 ];then
      min=1;
      let max=$batch_db_usrcnt-$min
      num=$(date +%s+%N);
      ((retnum=num%max+min));
      batch_db_usr="${batch_db_usr}_${retnum}"
   fi
   echo "$batch_db_pwd"
}
#########################################################################
#  brief: parse CTL
#version: 2017-01-01     create            lhzd863
#########################################################################
function splitctl()
{     
  ctlf="$1"

  #
  ctlf=`echo "$ctlf"|tr '[a-z]' '[A-Z]'`
  #job
  cal_job=`echo "$ctlf"|awk -F '.' '{print $1}'`
  #sys
  cal_sys=${cal_job:0:5}
  #
  cal_platform=${cal_job:6:4}
  #TABLE NAME
  cal_tbname=${cal_job:11}
  #date
  cal_date=`echo "$ctlf"|awk -F '.' '{print $2}'`
  vtime=`echo "$ctlf"|awk -F '.' '{print $3}'`
  if [ $# -gt 1 ];then
    #
    timestamp_offset_second="$2"
    #
    local tmp_date_year=${cal_date:0:4}

    local tmp_date_month=${cal_date:4:2}  
    local tmp_date_day=${cal_date:6:2}
    local tmp_time_hour=${vtime:0:2}
    local tmp_time_minute=${vtime:2:2}
    local tmp_time_second=${vtime:4:2}
    #
    local time2s_timestamp_start=$(date +%s -d "${tmp_date_year}-${tmp_date_month}-${tmp_date_day} ${tmp_time_hour}:${tmp_time_minute}:${tmp_time_second}")
    local time2s_timestamp_subval=$(($time2s_timestamp_start-$timestamp_offset_second))
    local tmp_timstamp_date=$(date +%Y-%m-%d\ %H:%M:%S -d "1970-01-01 UTC $time2s_timestamp_subval seconds")
    local tmp_cal_date=${tmp_timstamp_date:0:10}
    local tmp_cal_time=${tmp_timstamp_date:11:8}
    cal_date=`echo "$tmp_cal_date"|sed 's/-//g'`   
    vtime=`echo "$tmp_cal_time"|sed 's/://g'`
  fi
  #
  cal_hour=${vtime:0:2}
  #
  cal_minute=${vtime:2:2}
  #
  cal_second=${vtime:4:2}
  #
  cal_time="${cal_hour}${cal_minute}${cal_second}"
  #
  cal_date_0="${cal_date:0:4}-${cal_date:4:2}-${cal_date:6:2}"
  cal_time_0="${cal_hour}:${cal_minute}:${cal_second}"
  cal_year="${cal_date:0:4}"
  cal_month="${cal_date:4:2}"
  cal_day="${cal_date:6:2}"
  #
  logininfo
}
#########################################################################
#  brief: retry login
#version: 2017-01-01     create            lhzd863
#########################################################################
function retryLogin()
{
  cal_job="$1"
  
  if [ ${#cal_job} -eq 0 ];then
      echo "retry login fail"
      exit 1
  fi
  
  cal_sys=${cal_job:0:5}
  cal_platform=${cal_job:6:4}
  cal_tbname=${cal_job:11}
  #
  logininfo
}

#########################################################################
#  brief: parse cron expression
#version: 2017-01-01     create            lhzd863
#########################################################################
function isCronRunOk()
{
   local strCron="$1"
   local strTm="$2"
   
   java -jar ${BATCH_HOME}/lib/CronExpressionParse.jar  "$strCron" "$strTm"
   local ret=$?
   if [ $ret -ne 0 ];then 
      return $ret
   fi
   return 0
}
#########################################################################
#  brief: end quit
#version: 2017-01-01     create            lhzd863
#########################################################################
function endbt
{
  etst=`date +"%Y-%m-%d %H:%M:%S"`
  echo "exit $etst"
  exit 0
}

