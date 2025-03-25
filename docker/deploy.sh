#!/bin/ksh

docker swarm init

echo "password" | docker secret create pg_pass_file -
echo "password" | docker secret create pgadmin_pass_file -
echo "accesskey" | docker secret create market_stack_access_key -
echo "password" | docker secret create proxy_pass_file -

docker stack rm sdc
docker build -t stock_data_collector -f Dockerfile.sdc .
docker build -t postgres_sdc -f Dockerfile.pg .
docker build -t scraper -f Dockerfile.scraper .
# docker build -t pgadmin_sdc -f Dockerfile.pgadmin .
docker build -t openbb -f Dockerfile.openbb .
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