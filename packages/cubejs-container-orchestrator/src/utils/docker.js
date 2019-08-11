const util = require("util");
const exec = util.promisify(require("child_process").exec);

function run({ image, name, env, detached = true }) {
  const envString = Object.entries(env)
    .map(([k, v]) => `-e ${k}=${typeof v === "string" ? `"${v}"` : v}`)
    .join(" ");
  const detachedFlag = detached ? " -d " : "";
  return exec(
    `docker run --name ${name} ${envString} ${detachedFlag} ${image}`
  ).then(() => true);
}

const container = {
  exists(name) {
    return exec(`docker container inspect ${name}`)
      .then(() => true)
      .catch(() => false);
  },
  stop(name) {
    return exec(`docker container stop ${name}`)
      .then(() => true)
      .catch(() => false);
  },
};

module.exports = {
  run,
  container
};
