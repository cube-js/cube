view(`CompletedOrders`, {
  description: `Orders filtered by the status of being completed.`,
  includes: [
    // Measure
    Orders.completedCount,
    // Dimension
    Orders.createdAt,
  ],
});