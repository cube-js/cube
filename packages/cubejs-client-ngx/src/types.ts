interface ChartPivotFunc {
  (): any[];
}

interface SeriesNamesFunc {
  (): [{ key: string; title: string }];
}

export interface ResultSet {
  chartPivot: ChartPivotFunc;
  seriesNames: SeriesNamesFunc;
}

export interface MetaResult {
  cubes: [
    {
      title: string;
      name: string;
      measures: any[];
      dimensions: any[];
      segments: any[];
    }
  ];
  cubesMap: any;
  meta: any;
}
