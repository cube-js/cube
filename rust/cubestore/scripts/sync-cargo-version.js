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

const workspaceRoot = path.resolve(__dirname, '..');
execSync('cargo update --workspace', { cwd: workspaceRoot, stdio: 'inherit' });

const cargoLockPath = path.resolve(workspaceRoot, 'Cargo.lock');
execSync(`git add ${cargoTomlPath} ${cargoLockPath}`, { stdio: 'inherit' });
