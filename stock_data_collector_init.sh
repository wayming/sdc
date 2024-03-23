#/bin/bash


pgpass=`cat $PGPASSFILE | awk -F ":" '{print $5}'`
echo "export PGPASSWORD=$pgpass" > ~/.profile

msaccesskey=`cat $MSACCESSKEYFILE`
echo "export MSACCESSKEY=$msaccesskey" >> ~/.profile

sleep infinity