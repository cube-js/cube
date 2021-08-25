# echo 'Deleting the old build'

# rm -rf ../charts-dist/angular 2> /dev/null
# mkdir -p ../charts-dist/angular

# echo 'Scaffolding the app'
# node angular/index.js

echo 'Building the app'
cd ../charts-dist/angular/angular-charts && npm run build -- --prod
cd - || exit

echo 'Moving files'
rm -r ../public/chart-renderers/angular 2> /dev/null
mkdir -p ../public/chart-renderers/angular
node angular/move.js

echo 'Done!'
