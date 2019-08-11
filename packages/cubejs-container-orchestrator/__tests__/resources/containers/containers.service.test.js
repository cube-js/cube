jest.mock('../../../src/utils/docker');
jest.spyOn(global.console, "error").mockImplementation();

const uuidV4 = require('uuid/v4');
const docker = require('../../../src/utils/docker');
const containerService = require('../../../src/resources/containers/containers.service');

async function executeAndCatch(fn, ...args) {
  try {
    await fn(...args);
    return Promise.reject();
  } catch (err) {
    return Promise.resolve(err);
  }
}

describe(containerService.start.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it("given an invalid or missing `clientUUID`, should throw a validation Error", async () => {
    // arrange
    const config1 = {};
    const config2 = {
      clientUUID: 'some uuid',
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    const error1 = await executeAndCatch(containerService.start, config1);
    const error2 = await executeAndCatch(containerService.start, config2);

    // assert
    expect(error1).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"clientUUID" is required',
          type: "any.required",
        }
      ]
    });
    expect(error2).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message:
            '"clientUUID" with value "some uuid" fails to match the required pattern: /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i',
          type: "string.regex.base",
        }
      ]
    });
  });

  it("given an invalid or missing `applicationName`, should throw a validation Error", async () => {
    // arrange
    const config1 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
    };
    const config2 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: 12345,
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    const error1 = await executeAndCatch(containerService.start, config1);
    const error2 = await executeAndCatch(containerService.start, config2);

    // assert
    expect(error1).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"applicationName" is required',
          type: "any.required"
        }
      ]
    });
    expect(error2).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"applicationName" must be a string',
          type: "string.base"
        }
      ]
    });
  });

  it("given an invalid or missing `image`, should throw a validation Error", async () => {
    // arrange
    const config1 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: 'app',
    };
    const config2 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: 'app',
      image: 12345,
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    const error1 = await executeAndCatch(containerService.start, config1);
    const error2 = await executeAndCatch(containerService.start, config2);

    // assert
    expect(error1).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"image" is required',
          type: "any.required"
        }
      ]
    });
    expect(error2).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"image" must be a string',
          type: "string.base"
        }
      ]
    });
  });

  it("given an invalid or missing `env`, should throw a validation Error", async () => {
    // arrange
    const config1 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: 'mysql:latest',
    };
    const config2 = {
      clientUUID: uuidV4(),
      applicationName: "app",
      image: 'mysql:latest',
      env: 'env',
    };
    // act
    const error1 = await executeAndCatch(containerService.start, config1);
    const error2 = await executeAndCatch(containerService.start, config2);

    // assert
    expect(error1).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"env" is required',
          type: "any.required"
        }
      ]
    });
    expect(error2).toMatchObject({
      message: "ValidationError: Invalid configuration",
      status: 400,
      payload: [
        {
          message: '"env" must be an object',
          type: "object.base"
        }
      ]
    });
  });

  it("should throw if the container's hash is not in the state but a container with the same name exists", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    docker.container.exists.mockImplementationOnce(() => Promise.resolve(true));
    // act
    const error = await executeAndCatch(containerService.start, config);
    // assert
    expect(error).toMatchObject({
      message: `Error: docker container with name cubejs_test_app_01ccfeec7e8f11e810209cf73a4a58ac6990faff already active, but not in memory. Aborting.`,
      status: 500
    });
    expect(console.error).toHaveBeenCalledWith(error);
  });

  it("given that the container hash doesn't exist in the state, should call docker.run with the right name and config", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    await containerService.start(config);
    expect(docker.run.mock.calls[0][0]).toMatchObject({
      name: expect.stringMatching(new RegExp(`^cubejs_test_${config.applicationName}_([a-f0-9]{40})`)),
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      },
      detached: true,
    });
  });

  it("given that the container exists in the state and that the container is down, should call docker.run with the right name and config", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    // should start the docker container
    await containerService.start(config);
    // now container is still down according to mock, so should call it a second time
    await containerService.start(config);
    expect(docker.run).toHaveBeenCalledTimes(2);
    expect(docker.run.mock.calls[1][0]).toMatchObject({
      name: expect.stringMatching(
        new RegExp(`^cubejs_test_${config.applicationName}_([a-f0-9]{40})`)
      ),
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      },
      detached: true,
    });
  });

  it("should return the name, image, hash and env of the container", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    // act
    const result = await containerService.start(config);
    // assert
    expect(result).toStrictEqual({
      name: 'cubejs_test_app_01ccfeec7e8f11e810209cf73a4a58ac6990faff',
      image: "mysql:latest",
      hash: '01ccfeec7e8f11e810209cf73a4a58ac6990faff',
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    });
  });
});

describe(containerService.stop.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it('given an active container with dependents, should return false and not stop the container', async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    // act
    const result = await containerService.stop(hash);
    // assert
    expect(result).toBe(false);
    expect(docker.container.stop).not.toHaveBeenCalled();
    expect(containerService.getDependents(hash)).toStrictEqual([
      "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6"
    ]);
  });

  it("given an active container without dependents, should stop the container", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    containerService.removeDependent(config.clientUUID);
    // act
    const result = await containerService.stop(hash);
    // assert
    expect(result).toBe(true);
    expect(docker.container.stop).toHaveBeenCalledTimes(1);
    expect(containerService.getDependents(hash)).toMatchObject([]);
  });

  it("given a force=true argument, should stop the container", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    // act
    const result = await containerService.stop(hash, true);
    // assert
    expect(result).toBe(true);
    expect(docker.container.stop).toHaveBeenCalledTimes(1);
    expect(containerService.getDependents(hash)).toStrictEqual([]);
  });
});

describe(containerService.getDependents.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());
  it('given a container with dependent clientUUIDs, should return the list of said clientUUIDs, without duplicates', async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    await containerService.start(config);
    // act
    // assert
    expect(containerService.getDependents(hash)).toStrictEqual([
      "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
    ]);
  });
});

describe(containerService.hasDependents.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it("given a container with dependent clientUUIDs, should return true", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    // act
    // assert
    expect(containerService.hasDependents(hash)).toBe(true);
  });

  it("given a container without dependent clientUUIDs, should return false", async () => {
    // arrange
    const config = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const { hash } = await containerService.start(config);
    containerService.removeDependent("b9f9c1dd-4ad6-49ad-be80-378e830e6ad6");
    // act
    // assert
    expect(containerService.hasDependents(hash)).toBe(false);
  });

  it("given a container hash that does not exist should return false", async () => {
    // act
    // assert
    expect(containerService.hasDependents('some_hash')).toBe(false);
  });
});

describe(containerService.removeDependent.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it("should remove the provided dependent clientUUID from the containers with said dependent, without affecting other dependents for affected and unaffected containers", async () => {
    // arrange
    const config1 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "mysql:latest",
      env: {
        MYSQL_ROOT_PASSWORD: "example"
      }
    };
    const secondClientUUID = "221db8af-82de-431a-906f-7dbf5248c2af";
    const config2 = {
      clientUUID: "b9f9c1dd-4ad6-49ad-be80-378e830e6ad6",
      applicationName: "app",
      image: "postgres:latest",
      env: {
        POSTGRES_ENV_VAR: "example"
      }
    };
    const config3 = {
      clientUUID: "221db8af-82de-431a-906f-7dbf5248c2af",
      applicationName: "app",
      image: "mongo:latest",
      env: {
        MONGO_ENV_VAR: "example"
      }
    };
    // setting up two users for the same container
    const { hash: hash1 } = await containerService.start(config1);
    await containerService.start({
      ...config1,
      clientUUID: secondClientUUID,
    });
    const { hash: hash2 } = await containerService.start(config2);
    const { hash: hash3 } = await containerService.start(config3);
    // act
    const affectedContainers = containerService.removeDependent(config1.clientUUID);
    // assert
    // assert that it only removes the config1.clientUUID from dependents
    expect(containerService.getDependents(hash1)).toStrictEqual([
      secondClientUUID
    ]);
    // assert that it does so across all the containers that have config1.clientUUID as dependent
    expect(containerService.getDependents(hash2)).toStrictEqual([]);
    // assert that it does no affect other containers
    expect(containerService.getDependents(hash3)).toStrictEqual([
      secondClientUUID,
    ]);
    expect(affectedContainers).toStrictEqual([
      hash1,
      hash2,
    ]);
  });
});

describe(containerService.stopUnusedContainers.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it('should stop all the containers without dependents', async () => {
    // arrange
    const clientUUID = "221db8af-82de-431a-906f-7dbf5248c2af";
    containerService.__setState({
      dependents: {
        hash1: new Set([clientUUID]),
        hash2: new Set([]),
        hash3: new Set([]),
      },
      containers: {
        hash1: {},
        hash2: {},
        hash3: {},
      },
    });
    // act
    const affectedContainers = await containerService.stopUnusedContainers();
    // assert
    // assert that it only removes the config1.clientUUID
    expect(affectedContainers).toStrictEqual([
      { success: true, hash: "hash2" },
      { success: true, hash: "hash3" }
    ]);
    expect(docker.container.stop).toHaveBeenCalledTimes(2);
  });
});

describe(containerService.stopAll.name, () => {
  beforeEach(() => jest.clearAllMocks());
  afterEach(() => containerService.__resetState());

  it("should stop all the containers", async () => {
    // arrange
    const clientUUID = "221db8af-82de-431a-906f-7dbf5248c2af";
    containerService.__setState({
      dependents: {
        hash1: new Set([clientUUID]),
        hash2: new Set([]),
        hash3: new Set([])
      },
      containers: {
        hash1: {},
        hash2: {},
        hash3: {}
      }
    });
    // act
    const affectedContainers = await containerService.stopAll();
    // assert
    // assert that it only removes the config1.clientUUID
    expect(affectedContainers).toStrictEqual([
      { success: true, hash: "hash1" },
      { success: true, hash: "hash2" },
      { success: true, hash: "hash3" }
    ]);
    expect(docker.container.stop).toHaveBeenCalledTimes(3);
  });
});
