import { fetchOrdersSchema } from './fetchSchema';

asyncModule(async () => {
  const ordersSchema = await fetchOrdersSchema();

  cube(`Orders`, ordersSchema);

});
