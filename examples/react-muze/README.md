# Tableau like visualizations with MuzeJS

An example of how to create faceted charts using Muze and React on the frontend with CubeJS and PostgreSQL on the backend.

Demo: https://react-muze-demo.cube.dev/

## Running the Example

### Download and Import the Example Dataset

Ensure that a working instance of PostgreSQL is up on your system. The example dataset can be imported by running the following commands.

```
$ curl http://cube.dev/downloads/ecom-dump.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

### Backend
To start the backend CubeJS server use these commands
```
$ npm install
$ npm run dev
```
Visit `http://localhost:4000` in your browser.

### Frontend
To start frontend React application use these commands
```
$ cd dashboard-app
$ npm install
$ npm start
```
Visit `http://localhost:3000` in your browser.

For a more in-depth README for the React application, check out [dashboard-app/README.md](./dashboard-app/README.md).

