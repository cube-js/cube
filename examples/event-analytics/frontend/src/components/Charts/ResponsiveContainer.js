import React from 'react';
import { ResponsiveContainer } from 'recharts';

import {
  DASHBOARD_CHART_MIN_HEIGHT,
  RECHARTS_RESPONSIVE_WIDTH
} from './helpers.js';

const ResponsiveContainerComponent = ({ children }) => (
  <ResponsiveContainer width={RECHARTS_RESPONSIVE_WIDTH} height={DASHBOARD_CHART_MIN_HEIGHT}>
    { children }
  </ResponsiveContainer>
);

export default ResponsiveContainerComponent;
