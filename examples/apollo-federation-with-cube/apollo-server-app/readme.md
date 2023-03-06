# Run Development env

1. `npm i`
2. `npm run dev`

# Run Prod env for Node.js server

1. `npm i`
2. `npm start`

# Deploy
Build Docker containers and push then to your registry of choice.

1. `docker compose build`
2. `docker push <image>/server`
2. `docker push <image>/gateway`

# Add an SDL subgraph for Cube

- Create a Supergraph in Apollo Studio
- Run sample queries in Apollo Studio
- Open the SDL tab and download the file
- Create a file called `cube.graphql` and paste in the contents of the downloaded SDL file
- Run the rover command to add a subgraph to the Supergraph you created in Apollo Studio

```bash
APOLLO_KEY=service:Your-Graph:1234567890 \
  rover subgraph publish Your-Graph@current \
  --name cube --schema ./graphql/cube.graphql \
  --routing-url https://coloured-iguana.aws-eu-central-1.cubecloudapp.dev/cubejs-api/graphql
```

> Note: We have provided a Cube Cloud deployment for you to try out and add to the Supergraph.

# Add an SDL subgraph for the Apollo Server

- Create a file called `apollo.graphql`
- Add the SDL defintion from `fraud.js`
- Run the rover command to add a subgraph to the Supergraph you created in Apollo Studio

```bash
APOLLO_KEY=service:Your-Graph:1234567890 \
  rover subgraph publish Your-Graph@current \
  --name apollo --schema ./graphql/apollo.graphql \
  --routing-url http://35.192.83.151/graphql
```

> Note: We have provided an Apollo Server for you to try out and add to the Supergraph.

# Create/Download the Supergraph

You can create the Supergraph in two ways.

1. Download the Supergraph SDL from Apollo Studio

    - In Apollo Studio, open the SDL of the Supergraph you created
    - Click download
    - Create a file called `supergraph.graphql` and paste in the contents of the downloaded SDL file

2. Create a Supergraph file with `rover`
    
    - Create a Supergraph config file

        ```yaml
        federation_version: 2
        subgraphs:
          apolloserver:
            routing_url: http://35.192.83.151/graphql
            schema:
              file: ./apollo.graphql
          cube:
            routing_url: https://coloured-iguana.aws-eu-central-1.cubecloudapp.dev/cubejs-api/graphql
            schema:
              file: ./cube.graphql
        ```
    
    - Run the rover command below:
    
        ```bash
        rover supergraph compose --config ./graphql/supergraph.yaml > ./graphql/supergraph.graphql
        ```


Done! You now load the `supergraph.graphql` in the `gateway.js` and run the Apollo Gateway with a Federated Supergraph!

# Using Apollo GraphOS

Create a Supergraph and initial Subgraph for the Cube GraphQL API in Apollo Studio by following this [tutorial](https://www.apollographql.com/docs/graphos/getting-started/).

Next, add another subgraph for the Apollo Server.

```bash
rover subgraph publish cube-team@main \
  --schema "./apollo.graphql" \
  --name ApolloServer \
  --routing-url "https://apollo.examplescube.dev/graphql"
```
