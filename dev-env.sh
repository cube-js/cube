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

    # Building typescript packages first from root (it's referenced project)
    yarn tsc

    for package in packages/*; do
        if [ -d "$package" ]; then
            echo "Building $package..."
            cd "$package"
            yarn build 2>/dev/null || true
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
            # Registering a package for `yarn link <name>` is Yarn 1 only, so bypass the repo's Yarn 4
            YARN_IGNORE_PATH=1 yarn link
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

# Function to link packages to an existing project
link_project_packages() {
    local app_name=$1

    echo "Linking packages in the project..."
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

    install_root_dependencies
    build_packages
    link_packages
    if [ -n "$app_name" ]; then
        link_project_packages "$app_name"
    fi
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
    echo "  link           Link all packages and link them to a project"
    echo "                  Usage: ./dev-env.sh link [app_name]"
    echo "                  If argument is omitted, cube packages will be marked as linked"
    echo ""
    echo "  setup          Run all steps (install, build, link)"
    echo "                  Usage: ./dev-env.sh setup [app_name]"
    echo "                  If argument is given, packages are also linked to that existing project"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo ""
    echo "Examples:"
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
    "setup")
        setup "$2"
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

