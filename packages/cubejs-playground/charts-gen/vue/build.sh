echo 'Deleting the old build'

rm -rf ../charts-dist/vue 2> /dev/null
mkdir -p ../charts-dist/vue

echo 'Scaffolding the app'
node vue/index.js

echo 'Building the app'
cd ../charts-dist/vue/vue-charts && GENERATE_SOURCEMAP=false npm run build -- --skip-plugins @vue/cli-plugin-eslint
cd - || exit

echo 'Moving files'
rm -r ../public/chart-renderers/vue 2> /dev/null
mkdir -p ../public/chart-renderers/vue
node vue/move.js

echo 'Done!'
