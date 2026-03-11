const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const version = process.env.npm_package_version;
if (!version) {
  console.error('npm_package_version is not set');
  process.exit(1);
}

const cargoTomlPath = path.resolve(__dirname, '..', 'cubestore', 'Cargo.toml');
const content = fs.readFileSync(cargoTomlPath, 'utf8');
const updated = content.replace(/^(version\s*=\s*)"[^"]*"/m, `$1"${version}"`);

if (content === updated) {
  console.log(`Cargo.toml version already matches ${version}`);
  process.exit(0);
}

fs.writeFileSync(cargoTomlPath, updated);
console.log(`Updated Cargo.toml version to ${version}`);

execSync(`git add ${cargoTomlPath}`, { stdio: 'inherit' });
