// The same ratio owned by the view instead, alongside the cube-owned one, so
// both placements are exercised against a real database.
view(`RetailAnalysis`, {
  cubes: [
    {
      joinPath: ItemLocationSales,
      includes: [`salesAmount`],
    },
    {
      joinPath: SalesLineItem,
      includes: [`transactionsWithoutReturns`, { name: `aovBasket`, alias: `aovBasketFromCube` }],
    },
    {
      joinPath: Locations,
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
