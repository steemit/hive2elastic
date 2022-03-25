#!/bin/ash

c1=`ps aux | grep hive2elastic_post | grep -v grep | wc -l`
c2=`ps aux | grep hive2elastic_account | grep -v grep | wc -l`

if [[ $c1 -lt 1 ]]; then
  echo Status: 500
  echo Content-type:text/plain
  echo
  echo hive2elastic_post_not_healthy
  exit 1
fi

if [[ $c2 -lt 1 ]]; then
  echo Status: 500
  echo Content-type:text/plain
  echo
  echo hive2elastic_account_not_healthy
  exit 1
fi

echo Status: 200
echo Content-type:text/plain
echo
echo healthcheck
exit 0
