#/bin/bash

pgpass=`cat $PGPASSFILE | awk -F ":" '{print $5}'`
echo "export PGPASSWORD=$pgpass" > ~/.profile

msaccesskey=`cat $MSACCESSKEYFILE`
echo "export MSACCESSKEY=$msaccesskey" >> ~/.profile

proxykey=`cat $PROXYKEYFILE`
echo "export PROXYPASSWORD=$proxykey" >> ~/.profile

echo "export SDC_HOME=/home/appuser" >> ~/.profile

sleep infinity

/bin/bash

