
const createSuccessResponse = () => {
  return () => ({
    promise: jest.fn(() => {
      return {
        DbUser: 'userFromEnv',
        DbPassword: 'passwordFromAWS',
      };
    }),
  });
};

const createErrorResponse = () => {
  return () => ({
    promise: jest.fn(() => {
      throw new Error('Bad');
    })
  });
}

const mockSettings = {
  responseType: 'success',
};

const setResponseType = (responseType) => {
  mockSettings.responseType = responseType;
};

const createResponse = () => {
  return mockSettings.responseType === 'success' ?
    createSuccessResponse() :
    createErrorResponse();
}

const Redshift = function() {
  return {
    getClusterCredentials: createResponse(),
  };
};

// eslint-disable-next-line no-underscore-dangle
Redshift.__setResponseType = setResponseType;

module.exports = Redshift;
