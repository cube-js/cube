import { DriverTest } from './driverTest';
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
  queryingProductDimensions,
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

const productsTestSet = [queryingProductDimensions];

export const mainTestSet = [
  ...customersTestSet,
  ...productsTestSet
];
