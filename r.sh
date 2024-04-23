#!/bin/bash

mypid=$$
for i in {1..1000}
do
  sleep 1
  echo "iteration"$i
  wget https://stockanalysis.com/stocks/msft/ -O $mypid.html -o wget.log
  #wget  https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ -O $mypid.html -o wget.log
  #curl https://stockanalysis.com/stocks/aapl/financials/balance-sheet/ > $mypid.html
  #curl https://stockanalysis.com/stocks/rds.b > $mypid.html
  sed -n "s#.*marketCap:\"\(.*\)\",revenue:.*#\1#p" $mypid.html
  #sed -n "s#.*Revenue\(.*\).*#\1#p" $mypid.html
done
