# IN DEVELOPEMENT
## Install Teradata VM (tested on version 17.10)
- [Link to download the VM](https://downloads.teradata.com/download/database/teradata-express-for-vmware-player?_gl=1*9gfmgq*_ga*NTM5NzI5MzkuMTY1ODgyMjQzMA..*_ga_7PE2TMW3FE*MTY1OTMzNjk5NC43LjEuMTY1OTMzNzI5OS4w)

### When you are the VM check the following:
#### Check that you are connect to the internet
`$ ip a`

#### Check that your DB is connected
1. `pdestate -a`
2. a) You DB is online if you have the following output :
```
PDE state is RUN/STARTED.
DBS state is 5: Lofons are enabled - The system is quiescent
```

2. b) If you don't get the output in point (1).
try to delete the following file:
- `$ rm /var/opt/teradata/tdtemp/PanicLoop`
- Click on the **Start Teradata** executable file on your Desktop and wait a few minutes. Check if you DB is now online with (1)

# Make sure to follow these steps after forking the repo:
1. Run `yarn install` in the root directory.
2. Run `yarn build` in the root directory to build the frontend dependent packages. 
3. Run `yarn build` in `packages/cubejs-playground` to build the frontend.
4. Run `yarn tsc:watch` to start the TypeScript compiler in watch mode.
5. Run `yarn link` in `packages/cubejs-<pkg>` for the drivers and dependent packages you intend to modify. 
- yarn link in "cubejs-schema-compiler" package
- yarn link in "cubejs-backend-shared" package
- yarn link in "cubejs-jdbc-driver" package

6. Run `yarn install` in `packages/cubejs-<pkg>` to install dependencies for drivers and dependent packages.
- yarn install in "cubejs-schema-compiler" package
- yarn install in "cubejs-backend-shared" package
- yarn install in "cubejs-jdbc-driver" package

7. Run `yarn link @cubejs-backend/<pkg>` in `packages/cubejs-teradata-jdbc-driver` to link drivers and dependent packages.
- yarn link "@cubejs-backend/shared"
- yarn link "@cubejs-backend/jdbc-driver"
- yarn link "@cubejs-backend/schema-compiler"

8. Run `yarn install` in `packages/cubejs-teradata-jdbc-driver` to install the rest of the dependencies and install the terajdbc4.jar 

9. Run `yarn build` in `packages/cubejs-teradata-jdbc-driver`

10. To make sure that all packages will build correctly in the container, run `yarn lerna run build` in the root of the repo.
- If you build successfuly, you're ready to build your own docker image

# Build a docker image

0. (optional) Run `yarn lerna run build` in the root of the repo to check all dependencies are satisfied.

**All the following steps are done in `packages/cubejs-docker`.**

1. Run the following command to build your image.
- `docker build -t cubejs/cube:dev -f dev.Dockerfile ../../`

2. Once the image is built (it will take time), fill in the `docker-compose.yml` file.
Make sure that the following environement variables are filled.
- `CUBEJS_DB_TYPE=<db_type>` # should be `teradata` to connect to teradata DB.
- `CUBEJS_DB_NAME=<dbName>` 
- `CUBEJS_DB_HOST=<host>` #should be `host.docker.internal` for localhost
- `CUBEJS_DB_TERADATA_URL=jdbc:teradata://<teradata_db_ip>/USER=<teradata_user>,PASSWORD=<teradata_user_pwd>`
- `CUBEJS_DB_TERADATA_ACCEPT_POLICY=true` #accept the Terms and Conditions of Teradata

3. To start the container `docker compose up`

4. Once is live, visit [http://localhost:4000/](http://localhost:4000/) to access the cube playground.
