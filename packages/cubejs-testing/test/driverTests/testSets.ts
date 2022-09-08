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
  filteringCustomersCubeThird,
  filteringCustomersEndsWithFilterFirst,
  filteringCustomersEndsWithFilterSecond,
  filteringCustomersEndsWithFilterThird,
  filteringCustomersStartsWithAndDimensionsFirst,
  filteringCustomersStartsWithAndDimensionsSecond,
  filteringCustomersStartsWithAndDimensionsThird,
  filteringCustomersEndsWithFilterAndDimensionsFirst,
  filteringCustomersEndsWithFilterAndDimensionsSecond,
  filteringCustomersEndsWithFilterAndDimensionsThird,
} from './tests';

const customersTestSet = [
  queryingCustomersDimensions,
  queryingCustomersDimensionsAndOrder,
  queryingCustomerDimensionsAndLimitTest,
  queryingCustomersDimensionsAndTotal,
  queryingCustomersDimensionsOrderLimitTotal,
  queryingCustomersDimensionsOrderTotalOffset,
  queryingCustomersDimensionsOrderLimitTotalOffset,
  filteringCustomersCubeFirst,
  filteringCustomersCubeSecond,
  filteringCustomersCubeThird,
  filteringCustomersEndsWithFilterFirst,
  filteringCustomersEndsWithFilterSecond,
  filteringCustomersEndsWithFilterThird,
  filteringCustomersStartsWithAndDimensionsFirst,
  filteringCustomersStartsWithAndDimensionsSecond,
  filteringCustomersStartsWithAndDimensionsThird,
  filteringCustomersEndsWithFilterAndDimensionsFirst,
  filteringCustomersEndsWithFilterAndDimensionsSecond,
  filteringCustomersEndsWithFilterAndDimensionsThird,
];

export const mainTestSet = [
  ...customersTestSet
];
