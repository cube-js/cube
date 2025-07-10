import { getFixtures } from './getFixtures';
import { Customers, Products, ECommerce, BigECommerce, RetailCalendar } from '../dataset';

export function getCreateQueries(type: string, suf?: string): string[] {
  const { cast, tables } = getFixtures(type);
  return [
    Products.create(cast, tables.products, suf),
    Customers.create(cast, tables.customers, suf),
    ECommerce.create(cast, tables.ecommerce, suf),
    BigECommerce.create(cast, tables.bigecommerce, suf),
    RetailCalendar.create(cast, tables.retailcalendar, suf),
  ];
}
