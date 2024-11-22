#!/bin/bash

set -e

CURRENT_DIR=$(pwd)
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Change to the cube repo directory
cd "$SCRIPT_DIR"

# Step 1: Run yarn install in the root directory
echo "Running 'yarn install' in the root directory..."
yarn install
echo "Building all packages..."
yarn build

# Step 3: Link all packages
echo "Linking all packages..."
for package in packages/*; do
    if [ -d "$package" ]; then
        echo "Linking $package..."
        cd "$package"
        yarn link
        cd "$SCRIPT_DIR"
    fi
done

# Step 4: Ask for application name and database type
read -p "Enter the application name: " APP_NAME

# Get available database types from packages directory
db_types=()
for package in packages/cubejs-*-driver; do
    if [ -d "$package" ]; then
        db_name=$(basename "$package" | sed 's/cubejs-\(.*\)-driver/\1/')
        db_types+=("$db_name")
    fi
done

echo "Available database types:"
PS3='Please select the database type: '
select DB_TYPE in "${db_types[@]}"
do
    if [[ -n "$DB_TYPE" ]]; then
        break
    else
        echo "Invalid selection. Please try again."
    fi
done

# Change back to the original directory
cd "$CURRENT_DIR"

# Create new project using cubejs-cli
echo "Creating new project with name $APP_NAME and database type $DB_TYPE..."
node "$SCRIPT_DIR/packages/cubejs-cli/dist/src/index.js" create "$APP_NAME" -d "$DB_TYPE"

# Step 5: Link all packages in the new project
echo "Linking packages in the new project..."
cd "$APP_NAME"

for package in "$SCRIPT_DIR"/packages/*; do
    if [ -d "$package" ]; then
        package_name=$(node -p "require('$package/package.json').name")
        echo "Linking $package_name..."
        yarn link "$package_name"
    fi
done

echo "Project setup completed!"
echo "You can now run 'yarn dev' in the $APP_NAME directory to start your project."

# Change back to the original directory
cd "$CURRENT_DIR"
