view('TotalRevenuePerCustomer', {
	description: `Total revenue per customer`,
  shown: COMPILE_CONTEXT.permissions['finance'],

	includes: [
		Orders.totalRevenue,
		Users.company,
	],
});
