import { getFixtures } from './getFixtures';
import { Customers, Products, ECommerce } from '../dataset';

export function getCreateQueries(type: string, suf?: string): string[] {
  const { cast } = getFixtures(type);
  return [
    Products.create(cast, suf),
    Customers.create(cast, suf),
    ECommerce.create(cast, suf),
  ];
}
