#!/bin/bash
######################################################################
#  brief :template
#version :2017-01-01     create              lhzd863
######################################################################

#load common
source ${BATCH_HOME}/lib/batch-common.sh

######################################################################
#  brief :main
#version :2017-01-01     create              lhzd863
######################################################################
function main()
{
  sql="
select date;  
"
  mysqlcmd "$sql"
  errquit $?

  return 0
}
######################################################################
#entre
  if [ $# -ne 1 ];then
     echo "USAGE: job.yyyymmdd.HHMMSS" 
     echo " e.g.: NSRPT_PSQL_RPT_TEST.20170101.000000"
     exit 1
  fi
  ctlf="$1"
  #1.spare ctl file
  splitctl "$ctlf"
  #2.cron expression
  isCronRunOk "* * * * * ? *" "${cal_date_0} ${cal_time_0}"
  succquit $? "cron express fail" 
  #3.main
  main
  #4.endbt
  endbt

