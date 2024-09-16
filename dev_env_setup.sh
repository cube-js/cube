#!/bin/bash

set -e

CURRENT_DIR=$(pwd)
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Change to the cube repo directory
cd "$SCRIPT_DIR"

# Step 1: Run yarn install in the root directory
echo "Running 'yarn install' in the root directory..."
yarn install

# Step 2: Run yarn build in the root directory
echo "Running 'yarn build' in the root directory..."
yarn build

# Step 3: Run yarn build in packages/cubejs-playground
echo "Running 'yarn build' in packages/cubejs-playground..."
cd packages/cubejs-playground
yarn build
cd ../..

# Step 4: Run yarn tsc for the first time
echo "Running 'yarn tsc --build'..."
yarn tsc

# Step 5: List available drivers and ask user to select
echo "Listing available drivers..."
available_drivers=$(ls packages | grep "driver")

PS3='Please select the drivers you want to use (enter number, then press Enter): '

# Display drivers without the prefix "cubejs-"
select selected_driver in $(echo "$available_drivers" | sed 's/cubejs-//') "Finish selection"
do
    if [[ "$selected_driver" == "Finish selection" ]]; then
        break
    fi
    selected_drivers+=("$selected_driver")
    echo "Selected drivers: ${selected_drivers[*]}"
done

# Step 6-7: Run yarn link and yarn install in packages/cubejs-<pkg>
for driver in "${selected_drivers[@]}"
do
    echo "Linking and installing dependencies for $driver..."
    cd "packages/cubejs-$driver"
    yarn link
    yarn install
    cd ../..
done

# Step 8: Run yarn link @cubejs-backend/<pkg> in packages/cubejs-server-core
cd packages/cubejs-server-core
for driver in "${selected_drivers[@]}"
do
    echo "Linking @cubejs-backend/$driver in packages/cubejs-server-core..."
    yarn link @cubejs-backend/"$driver"
done
cd ../..

# Step 9: Run yarn link in packages/cubejs-server-core
echo "Running 'yarn link' in packages/cubejs-server-core..."
cd packages/cubejs-server-core
yarn link
cd ../..

# Change back to the original directory
cd "$CURRENT_DIR"

# Step 10: Ask user if they want to create a new test project
read -p "Do you want to create a new test project? (yes/no, default: yes): " CREATE_PROJECT
CREATE_PROJECT=${CREATE_PROJECT:-yes}

if [[ "$CREATE_PROJECT" == "yes" || "$CREATE_PROJECT" == "y" ]]; then
    read -p "Enter the application name: " APP_NAME

    # List of available database types (hardcoded for now as of https://cube.dev/docs/reference/cli)
    db_types=("postgres" "mysql" "athena" "mongodb" "bigquery" "redshift" "mssql" "clickhouse" "snowflake" "presto" "druid")

    echo "Listing available database types..."
    PS3='Please select the database type: '
    select DB_TYPE in "${db_types[@]}"
    do
        if [[ -n "$DB_TYPE" ]]; then
            break
        else
            echo "Invalid selection. Please try again."
        fi
    done

    # Create new project using cubejs-cli
    echo "Creating new project with name $APP_NAME and database type $DB_TYPE..."
    npx cubejs-cli create "$APP_NAME" -d "$DB_TYPE"

    # Step 11: Run yarn link @cubejs-backend/server-core in your project directory
    echo "Linking @cubejs-backend/server-core in the project directory..."
    cd "$APP_NAME"
    yarn link @cubejs-backend/server-core
    cd ../
else
    echo "Ok. No problem!"
    echo "You need to run 'yarn link @cubejs-backend/server-core' in your project directory manually"
fi

# Step 11: Ask user if they plan to make changes to Rust code
read -p "Do you plan to make changes to Rust code? (yes/no, default: no): " RUST_CHANGES
RUST_CHANGES=${RUST_CHANGES:-no}

if [[ "$RUST_CHANGES" == "yes" || "$RUST_CHANGES" == "y" ]]; then
    # Run yarn link:dev in the script directory
    cd "$SCRIPT_DIR"
    echo "Running 'yarn link:dev' in the root directory..."
    yarn link:dev

    if [[ "$CREATE_PROJECT" == "yes" || "$CREATE_PROJECT" == "y" ]]; then
        dev_pkgs=("@cubejs-backend/shared"
                  "@cubejs-backend/cloud"
                  "@cubejs-backend/native"
                  "@cubejs-backend/server"
                  "@cubejs-backend/server-core"
                  "@cubejs-backend/api-gateway"
                  "@cubejs-backend/schema-compiler"
                  "@cubejs-backend/query-orchestrator"
                  "@cubejs-backend/athena-driver"
                  "@cubejs-backend/duckdb-driver"
                  "@cubejs-backend/bigquery-driver"
                  "@cubejs-backend/postgres-driver"
                  "@cubejs-backend/databricks-jdbc-driver"
                  "@cubejs-backend/mssql-driver"
                  "@cubejs-backend/clickhouse-driver"
                  "@cubejs-backend/snowflake-driver"
                  "@cubejs-backend/cubestore-driver"
                  "@cubejs-backend/templates"
                  "@cubejs-client/core"
                  "@cubejs-client/ws-transport"
                  "@cubejs-client/playground"
                  )

        cd "$CURRENT_DIR/$APP_NAME"
        echo "Linking dev packages in $APP_NAME project..."

        for pkg in "${dev_pkgs[@]}"
        do
            echo "Linking $pkg..."
            yarn link "$pkg"
        done
    else
        echo "Don't forget to link packages that you plan to modify inside your project!"
    fi
fi

echo "All steps completed successfully!"
echo "Run 'yarn dev' to start your testing project and verify changes."
