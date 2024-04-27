#!/bin/bash

url=$1
mypid=$$
log=wget$mypid.log
for i in {1..1000}
do
  randomNumber=$(( $RANDOM % 2 )).$(( $RANDOM % 10 ))
  sleep $randomNumber
  echo $randomNumber >> $log
  echo "iteration"$i >> $log
  wget $url -O $mypid.html -a $log
  if [ $? -eq 8 ]
  then
    echo "Delay 70 seconds"
    sleep 70
    continue
  fi
  #wget  https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ -O $mypid.html -o wget.log
  #curl https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ > $mypid.html
  #curl https://stockanalysis.com/stocks/rds.b > $mypid.html
  sed -n "s#.*marketCap:\"\(.*\)\",revenue:.*#\1#p" $mypid.html >> $log
  #sed -n "s#.*Revenue\(.*\).*#\1#p" $mypid.html
  rm -f $mypid.html
done
