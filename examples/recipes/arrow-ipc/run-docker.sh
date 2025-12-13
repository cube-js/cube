#localhost/cubejs/cube:mine

docker run -d -p 3000:3000 -p 4000:4000 \
       -e CUBEJS_DB_HOST=postgres://localhost \
       -e CUBEJS_DB_NAME=<DB_NAME> \
       -e CUBEJS_DB_USER=<USER> \
       -e CUBEJS_DB_PASS=<PASS> \
       -e CUBEJS_DB_TYPE=<DB_TYPE> \
       -e CUBEJS_API_SECRET=<API_SECRET> \
       -v $(pwd):/cube/conf \
       localhost/cubejs/cube:mine
#       cubejs/cube:latest
