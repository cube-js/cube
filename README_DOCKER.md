sudo docker build -t shlokkapoor/cubejs:latest -f ./packages/cubejs-docker/dev.Dockerfile .
sudo docker push shlokkapoor/cubejs:latest


sudo docker build -t shlokkapoor/cubejs:beta -f ./packages/cubejs-docker/dev.Dockerfile .
sudo docker push shlokkapoor/cubejs:beta