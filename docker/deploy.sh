#!/bin/ksh

docker stack rm sdc
docker build -t stock_data_collector -f Dockerfile.sdc .
docker build -t postgres_sdc -f Dockerfile.pg .
while [ 1 ];
do
    docker stack ps sdc
    if [ $? -eq 0 ]; then
        echo "Waiting stack sdc to be terminated"
        sleep 2
    else
        break
    fi
done
docker stack deploy --resolve-image=always -c docker-compose.debug.yml sdc