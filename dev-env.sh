#!/bin/bash

set -e

CURRENT_DIR=$(pwd)
SCRIPT_DIR=$(dirname "$(realpath "$0")")

IGNORED_PACKAGES=("cubejs-testing*" "cubejs-linter" "cubejs-docker")

# Function to install dependencies in root
install_root_dependencies() {
    echo "Running 'yarn install' in the root directory..."
    cd "$SCRIPT_DIR"
    yarn install
}

# Function to build all packages
build_packages() {
    echo "Building all packages..."
    cd "$SCRIPT_DIR"
    for package in packages/*; do
        if [ -d "$package" ]; then
            echo "Building $package..."
            cd "$package"
            if ! yarn build 2>/dev/null; then
                #echo "yarn build failed for $package, trying yarn tsc..."
                yarn tsc 2>/dev/null || true
            fi
            cd "$SCRIPT_DIR"
        fi
    done
}

# Function to create yarn links for all packages
link_packages() {
    echo "Linking all packages..."
    cd "$SCRIPT_DIR"
    for package in packages/*; do
        if [ -d "$package" ]; then
            package_name=$(basename "$package")

            skip_package="false"
            for pattern in "${IGNORED_PACKAGES[@]}"; do
                # shellcheck disable=SC2053
                if [[ "$package_name" == $pattern ]]; then
                    echo "Skipping $package_name..."
                    skip_package="true"
                fi
            done

            if [ "$skip_package" = "true" ]; then
                continue
            fi

            echo "Linking $package..."
            cd "$package"
            yarn link
            cd "$SCRIPT_DIR"
        fi
    done
}

# Function to get available database types
get_db_types() {
    cd "$SCRIPT_DIR"
    db_types=()
    for package in packages/cubejs-*-driver; do
        if [ -d "$package" ]; then
            db_name=$(basename "$package" | sed 's/cubejs-\(.*\)-driver/\1/')
            if [ "$db_name" != "base" ]; then
                db_types+=("$db_name")
            fi
        fi
    done
    printf "%s\n" "${db_types[@]}"
}

# Function to create new project
create_project() {
    local app_name=$1
    local db_type=$2

    # If app_name is not provided, ask for it
    if [ -z "$app_name" ]; then
        read -r -p "Enter the application name: " app_name
    fi

    # If db_type is not provided, show selection menu
    if [ -z "$db_type" ]; then
        # Get available database types
        db_types=()
        while IFS= read -r line; do
          db_types+=("$line")
        done < <(get_db_types)

        echo "Available database types:"
        PS3='Please select the database type: '
        select DB_TYPE in "${db_types[@]}"
        do
            if [[ -n "$DB_TYPE" ]]; then
                db_type=$DB_TYPE
                break
            else
                echo "Invalid selection. Please try again."
            fi
        done
    fi

    cd "$CURRENT_DIR"
    echo "Creating new project with name $app_name and database type $db_type..."
    node "$SCRIPT_DIR/packages/cubejs-cli/dist/src/index.js" create "$app_name" -d "$db_type"
    link_project_packages "$app_name"

    echo "Project setup completed!"
    echo "You can now run 'yarn dev' in the $app_name directory to start your project."
}

# Function to link packages to new project
link_project_packages() {
    local app_name=$1

    echo "Linking packages in the new project..."
    cd "$CURRENT_DIR/$app_name"
    for package in "$SCRIPT_DIR"/packages/*; do
        if [ -d "$package" ]; then
            package_name=$(basename "$package")

            skip_package="false"
            for pattern in "${IGNORED_PACKAGES[@]}"; do
                # shellcheck disable=SC2053
                if [[ "$package_name" == $pattern ]]; then
                    echo "Skipping $package_name..."
                    skip_package="true"
                fi
            done

            if [ "$skip_package" = "true" ]; then
                continue
            fi

            package_name=$(node -p "require('$package/package.json').name")
            echo "Linking $package_name..."
            yarn link "$package_name"
        fi
    done
}

# Main execution function
setup() {
    local app_name=$1
    local db_type=$2

    install_root_dependencies
    build_packages
    link_packages
    create_project "$app_name" "$db_type"
}

# Function to show help
show_help() {
    echo "Development environment setup script for Cube"
    echo ""
    echo "Usage: ./dev-env.sh <command> [arguments]"
    echo ""
    echo "Commands:"
    echo "  install         Install dependencies in root directory"
    echo "                  Usage: ./dev-env.sh install"
    echo ""
    echo "  build          Build all packages"
    echo "                  Usage: ./dev-env.sh build"
    echo ""
    echo "  drivers        List available database drivers"
    echo "                  Usage: ./dev-env.sh drivers"
    echo ""
    echo "  create         Create a new project"
    echo "                  Usage: ./dev-env.sh create [app_name] [db_type]"
    echo "                  If arguments are omitted, will ask interactively"
    echo ""
    echo "  link           Link all packages and link them to a project"
    echo "                  Usage: ./dev-env.sh link [app_name]"
    echo "                  If argument is omitted, cube packages will be marked as linked"
    echo ""
    echo "  setup          Run all steps (install, build, link, create project)"
    echo "                  Usage: ./dev-env.sh setup [app_name] [db_type]"
    echo "                  If arguments are omitted, will ask interactively"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./dev-env.sh create my-app postgres"
    echo "  ./dev-env.sh setup my-app"
    echo "  ./dev-env.sh link my-app"
}

command=$1

# Show help if no command provided
if [ -z "$command" ]; then
    show_help
    exit 0
fi

case "$command" in
    "install")
        install_root_dependencies
        ;;
    "build")
        build_packages
        ;;
    "link")
        link_packages
        if [ -n "$2" ]; then
            link_project_packages "$2"
        fi
        ;;
    "drivers")
        get_db_types
        ;;
    "create")
        create_project "$2" "$3"
        ;;
    "setup")
        setup "$2" "$3"
        ;;
    "-h"|"--help"|"help")
        show_help
        ;;
    *)
        echo "Error: Unknown command '$command'"
        echo ""
        show_help
        exit 1
        ;;
esac

cd "$CURRENT_DIR"

