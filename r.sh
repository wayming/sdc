#!/bin/bash

url=$1
nums=$2
mypid=$$
log=wget$mypid.log
i=0
while [ $i -le $nums ]
do
  # sleep 1
  echo "iteration"$i
  wget $url -e use_proxy=yes -e https_proxy=37.19.220.182:8443 -O $mypid.html -a $log
  #  wget $url -e use_proxy=yes -e https_proxy=136.226.230.84:10008 -O $mypid.html -a $log
  if [ $? -eq 8 ]
  then
    echo "Delay 20 seconds"
    sleep 20
    continue
  fi
  # https://stockanalysis.com/stocks/msft
  #wget  https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ -O $mypid.html -o wget.log
  #curl https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ > $mypid.html
  #curl https://stockanalysis.com/stocks/rds.b > $mypid.html
  sed -n "s#.*marketCap:\"\(.*\)\",revenue:.*#\1#p" $mypid.html >> $log
  #sed -n "s#.*Revenue\(.*\).*#\1#p" $mypid.html
  rm -f $mypid.html
  ((i++))
done
