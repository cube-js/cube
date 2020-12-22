echo 'Deleting the old build'

rm -rf ../charts-dist/react 2> /dev/null
mkdir -p ../charts-dist/react

echo 'Scaffolding the app'
node react/index.js

echo 'Building the app'
cd ../charts-dist/react/react-charts && SKIP_PREFLIGHT_CHECK=true GENERATE_SOURCEMAP=false npm run build
cd -

echo 'Moving files'
rm -r ../public/chart-renderers/react 2> /dev/null
mkdir -p ../public/chart-renderers/react
node react/move.js

echo 'Done!'