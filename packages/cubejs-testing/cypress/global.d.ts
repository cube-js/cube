/// <reference types="cypress" />

declare namespace Cypress {
  type MatchImageSnapshotOptions = {
    failureThreshold: number;
    failureThresholdType: "percent";
  };

  interface Chainable {
    getByTestId(dataTestAttribute: string, args?: any): Chainable<JQuery<HTMLElement>>;

    getByQa(dataTestAttribute: string, args?: any): Chainable<JQuery<HTMLElement>>;

    setQuery(query: Object, args?: any): void;

    setChartType(chartType: string): void;

    addMeasure(measureName: string): void;

    addDimension(dimensionName: string): void;

    runQuery(timeout?: number): void;

    matchImageSnapshot(
      name?: string,
      options?: Partial<
        MatchImageSnapshotOptions & Loggable & Timeoutable & ScreenshotOptions
      >
    ): void;
    matchImageSnapshot(
      options: Partial<
        MatchImageSnapshotOptions & Loggable & Timeoutable & ScreenshotOptions
      >
    ): void;

    getLocalStorage(item: string): Chainable<string | null>;
  }
}

declare module 'cypress-image-snapshot/command';
