#/bin/bash


pgpass=`cat $PGPASSFILE | awk -F ":" '{print $5}'`
echo "export PGPASSWORD=$pgpass" >> ~/.profile

sleep infinity