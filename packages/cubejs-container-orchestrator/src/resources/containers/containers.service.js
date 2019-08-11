const hashObject = require('object-hash');
const Joi = require('@hapi/joi');

// local modules
const docker = require('../../utils/docker');

// constants
var state = {
  containers: {},
  dependents: {},
};

module.exports = process.env.NODE_ENV === "test" ?
  {
    __resetState: () => {
      state = {
        containers: {},
        dependents: {}
      };
    },
    __getState: () => state,
    __setState: (newState) => {
      state = newState;
    },
    start,
    stop,
    stopAll,
    hasDependents,
    getDependents,
    removeDependent,
    getContainers,
    stopUnusedContainers
  } :
  {
    start,
    stop,
    stopAll,
    hasDependents,
    getDependents,
    removeDependent,
    getContainers,
    stopUnusedContainers
  };

// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////

async function start(config) {
  const uuidV4Regex = /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i;
  const startSchema = Joi.object().keys({
    clientUUID: Joi.string().regex(uuidV4Regex).required(),
    applicationName: Joi.string().required(),
    // @see https://stackoverflow.com/a/39672069/4442749
    // for some regexes. Problem is, current JS RegExp spec doesn't support it
    image: Joi.string().required(),
    env: Joi.object().required()
  });

  const validation = startSchema.validate(config);
  if (validation.error !== null) {
    const error = new Error("ValidationError: Invalid configuration");
    error.status = 400;
    error.payload = validation.error.details;
    throw error;
  }

  const { clientUUID, applicationName, image, env } = config;
  const hash = hashObject({ image, env });
  const name = `cubejs_test_${applicationName}_${hash}`;
  const containerExists = await docker.container.exists(name);

  if (!state.containers[hash] && containerExists) {
    const error = new Error(
      `Error: docker container with name ${name} already active, but not in memory. Aborting.`
    );
    error.status = 500;
    console.error(error);
    throw error;
  } else if (!state.containers[hash] && !containerExists) {
    await docker.run({ image, name, env, detached: true });
  } else if (state.containers[hash] && !containerExists) {
    // the container has been shutdown since we first started it
    await docker.run({ image, name, env, detached: true });
  } else {
    // (state.containers[hash] && containerExists)
    // do nothing
  }

  if (!state.containers[hash]) {
    state.containers[hash] = {
      image,
      name,
      env
    };
    state.dependents[hash] = new Set();
  }
  state.dependents[hash].add(clientUUID);
  return {
    name,
    image,
    hash,
    env
  };
}

async function stop(hash, force = false) {
  if (hasDependents(hash) && !force) {
    return false;
  }
  const { name } = state.containers[hash];
  await docker.container.stop(name);
  delete state.containers[hash];
  delete state.dependents[hash];
  return true;
}

function stopAll() {
  const containers = Object.keys(state.containers);
  const promises = containers.map(hash => stop(hash, true).then(success => ({ hash, success })));
  return Promise.all(promises);
}

function hasDependents(hash) {
  return state.dependents[hash] instanceof Set && state.dependents[hash].size !== 0;
}

function getDependents(hash) {
  if (hasDependents(hash)) {
    return [...state.dependents[hash]];
  }
  return [];
}

function removeDependent(clientUUID) {
  const affectedContainers = [];
  state.dependents = Object.entries(state.dependents).reduce((acc, [hash, dependents]) => {
    const newDependents = new Set(dependents);
    newDependents.delete(clientUUID);
    if (newDependents.size === dependents.size - 1) {
      affectedContainers.push(hash);
    }
    return {
      ...acc,
      [hash]: newDependents,
    };
  }, {});
  return affectedContainers;
}

function getContainers(clientUUID) {
  if (clientUUID === undefined) {
    return Object.values(state.containers);
  }
  return Object.entries(state.dependents)
    // eslint-disable-next-line no-unused-vars
    .filter(([hash, dependents]) => dependents instanceof Set && dependents.has(clientUUID))
    .map(([hash]) => state.containers[hash]);
}

function stopUnusedContainers() {
  const containers = Object.keys(state.containers);
  const promises = containers
    .filter(hash => !hasDependents(hash))
    .map(hash => stop(hash).then(success => ({ hash, success })));
  return Promise.all(promises);
}
