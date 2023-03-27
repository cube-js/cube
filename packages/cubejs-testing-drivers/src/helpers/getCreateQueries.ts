import { getFixtures } from './getFixtures';
import { Customers, Products, ECommerce } from '../dataset';

export function getCreateQueries(type: string): string[] {
  const { cast } = getFixtures(type);
  return [
    Products.create(cast),
    Customers.create(cast),
    ECommerce.create(cast),
  ];
}
