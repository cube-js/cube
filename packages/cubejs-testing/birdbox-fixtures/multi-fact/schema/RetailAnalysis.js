// The same ratio owned by the view instead, alongside the cube-owned one, so
// both placements are exercised against a real database.
view(`RetailAnalysis`, {
  cubes: [
    {
      join_path: ItemLocationSales,
      includes: [`salesAmount`],
    },
    {
      join_path: SalesLineItem,
      includes: [`transactionsWithoutReturns`, { name: `aovBasket`, alias: `aovBasketFromCube` }],
    },
    {
      join_path: Locations,
      includes: [`region`],
    },
  ],

  measures: {
    aovBasket: {
      sql: `${CUBE.salesAmount} / NULLIF(${CUBE.transactionsWithoutReturns}, 0)`,
      type: `number`,
      multiStage: true,
    },
  },
});
