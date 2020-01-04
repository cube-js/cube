import React from 'react';
import * as PropTypes from 'prop-types';
import CubeContext from './CubeContext';

const CubeProvider = ({ cubejsApi, children }) => (
  <CubeContext.Provider value={{ cubejsApi }}>
    {children}
  </CubeContext.Provider>
);

CubeProvider.propTypes = {
  cubejsApi: PropTypes.object.isRequired,
  children: PropTypes.any.isRequired
};

export default CubeProvider;
