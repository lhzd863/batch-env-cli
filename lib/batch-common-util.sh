#########################################################################
#  brief: common functon
#version: 2017-01-01     create            lhzd863
#########################################################################


#########################################################################
#  brief: replace variable
#version: 2017-01-01     create            lhzd863
#########################################################################
function replacevariable()
{     
  local str="$1"
  local strkey="$2"
  local strval="$3"

  local retstr=`echo "$str"|sed "s/\[${strkey}\]/${strval}/g"` 
  #
  echo "$retstr"
}

#########################################################################
#  brief: offset timestamp
#version: 2017-01-01     create            lhzd863
#########################################################################
function offsetTimestamp()
{
  local str_basic_timestamp="$1"
  local str_sub_second="$2"
  local basic_second=$(date +%s -d "${str_basic_timestamp}")
   
  local ret_timestamp=$(($basic_second-$str_sub_second))
  local rettm=$(date +%Y-%m-%d\ %H:%M:%S -d "1970-01-01 UTC $ret_timestamp seconds")
  #
  echo "$rettm"
}


