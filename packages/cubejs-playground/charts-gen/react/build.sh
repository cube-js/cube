mkdir -p ../charts-dist/react

echo 'Scaffolding the app'
node react/index.js

echo 'Building the app'
cd ../charts-dist/react/react-charts && SKIP_PREFLIGHT_CHECK=true npm run build
cd -

echo 'Moving files'
rm -r ../public/chart-renderers/react 2> /dev/null
mkdir -p ../public/chart-renderers/react

ls -l ../charts-dist/react/react-charts/build/static
mv ../charts-dist/react/react-charts/build/static ../public/chart-renderers/react
mv ../charts-dist/react/react-charts/build/index.html ../public/chart-renderers/react