const os = require('os');
const dns = require('dns');
const assert = require("assert");
const util = require('util');
const exec = util.promisify(require('child_process').exec);

// community packages
const R = require('ramda');
const commandExists = require('command-exists');
const disk = require('diskusage');
const executable = require('executable');
const which = util.promisify(require('which'));

dns.resolve = util.promisify(dns.resolve);

const MINIMAL_DOCKER_VERSION = [18, 0, 0];
const MINIMAL_DISK_SPACE_REQUIRED_BYTES = 1024 ** 3;

module.exports = {
  assertStartConditions,
};

// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////

async function assertStartConditions() {
  await assertEnv();
  await assertDocker();
  await assertNetwork();
  await assertDiskSpace();
}

/**
 * Is the environment properly setup?
 */
function assertEnv() {
  assert(process.env.CUBEJS_TEST_PORT, "No port specified");
}

async function assertDocker() {
  assert(
    await dockerExists(),
    `Requires docker version >=${MINIMAL_DOCKER_VERSION.join(".")}`
  );
  assert(
    await hasPermissionToRunDocker(),
    "User does not have the necessary permissions to run docker"
  );
  assert(
    await hasMinimalRequiredDockerVersion(),
    `Requires docker version >=${MINIMAL_DOCKER_VERSION.join('.')}`
  );
}

function dockerExists() {
  return commandExists("docker")
    .then(() => true)
    .catch(() => false);
}

async function hasPermissionToRunDocker() {
  const dockerPath = await which('docker');
  return executable(dockerPath);
}

async function hasMinimalRequiredDockerVersion() {
  const versionString = (await exec("docker --version")).stdout.match(/\d+\.\d+\.\d+/);
  if (versionString === null) {
    return false;
  }
  const [major] = versionString[0].split(".").map(v => parseInt(v, 10));
  return major >= MINIMAL_DOCKER_VERSION[0];
}

/**
 * Is network available?
 * @see https://nodejs.org/api/os.html#os_os_networkinterfaces
 */
async function assertNetwork() {
  const interfaces = getNetworkInterfaceArray();
  assert(
    interfaces.some((iface) => iface.internal),
    "Cannot boot docker without an internal network interface like local loopback (localhost)"
  );
  assert(
    interfaces.some((iface) => !iface.internal),
    "Cannot connect to the internet without an external network interface"
  );
  assert(
    await canAccessInternet(),
    "Cannot access internet. Check your internet access."
  );
}

function getNetworkInterfaceArray() {
  return R.pipe(
    Object.values,
    R.flatten
  )(os.networkInterfaces());
}

/**
 * @see https://stackoverflow.com/questions/15270902/check-for-internet-connectivity-in-nodejs
 */
function canAccessInternet() {
  return dns.resolve('google.com')
    .then(() => true)
    .catch(() => false);
}

/**
 * Checks for a minimum of 1GB of free disk space to load at least two images.
 * Example image sizes:
 * | Date | image | size |
 * |------|-------|--------|
 * | 2019-08-10 | mongo:latest | 413MB |
 * | 2019-08-10 | mysql:5.7 | 373MB |
 * | 2019-08-10 | postgres:alpine | 72.5MB |
 */
async function assertDiskSpace() {
  assert(
    (await disk.check(process.cwd())).available >= MINIMAL_DISK_SPACE_REQUIRED_BYTES,
    "Insufficient disk space to run the Container Orchestrator. Have at least 1GB available."
  );
}
