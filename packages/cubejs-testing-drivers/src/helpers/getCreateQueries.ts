import { getFixtures } from './getFixtures';
import { Customers, Products, ECommerce } from '../dataset';

export function getCreateQueries(type: string, suf?: string): string[] {
  const { cast, tables } = getFixtures(type);
  return [
    Products.create(cast, tables.products, suf),
    Customers.create(cast, tables.customers, suf),
    ECommerce.create(cast, tables.ecommerce, suf),
  ];
}
