import {
  queryingCustomersDimensions,
  queryingCustomersDimensionsAndOrder,
  queryingCustomerDimensionsAndLimitTest,
  queryingCustomersDimensionsAndTotal,
  queryingCustomersDimensionsOrderLimitTotal,
  queryingCustomersDimensionsOrderTotalOffset,
  queryingCustomersDimensionsOrderLimitTotalOffset,
  filteringCustomersCubeFirst,
  filteringCustomersCubeSecond,
  filteringCustomersCubeThird
} from './tests';

export const mainTestSet = [
  queryingCustomersDimensions,
  queryingCustomersDimensionsAndOrder,
  queryingCustomerDimensionsAndLimitTest,
  queryingCustomersDimensionsAndTotal,
  queryingCustomersDimensionsOrderLimitTotal,
  queryingCustomersDimensionsOrderTotalOffset,
  queryingCustomersDimensionsOrderLimitTotalOffset,
  filteringCustomersCubeFirst,
  filteringCustomersCubeSecond,
  filteringCustomersCubeThird
];
