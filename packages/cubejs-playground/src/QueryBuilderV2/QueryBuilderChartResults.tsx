import { Grid, Title, Paragraph, tasty } from '@cube-dev/ui-kit';
import { ChartType, PivotConfig, Query, ResultSet } from '@cubejs-client/core';
import { RefObject } from 'react';

import { PlaygroundChartRenderer } from './components/ChartRenderer';

interface QueryBuilderChartResultsProps {
  resultSet: ResultSet<any> | null;
  isLoading: boolean;
  query: Query;
  pivotConfig: PivotConfig;
  chartType: ChartType;
  isExpanded: boolean;
  overflow?: string;
  containerRef?: RefObject<HTMLDivElement>;
}

const MAX_HEIGHT = 350;
const MAX_SERIES_LIMIT = 25;

const ChartContainer = tasty({
  qa: 'QueryBuilderChart',
  styles: {
    overflow: 'hidden',
    padding: '1x 1x 0 0',
    styledScrollbar: true,
  },
});

export function QueryBuilderChartResults({
  resultSet,
  isLoading,
  query,
  pivotConfig,
  chartType,
  isExpanded,
  containerRef,
  overflow = 'clip',
}: QueryBuilderChartResultsProps) {
  const isChartTooBig = resultSet && resultSet?.seriesNames(pivotConfig).length > MAX_SERIES_LIMIT;

  if (resultSet && !isLoading && isExpanded) {
    if (isChartTooBig) {
      return (
        <Grid height={MAX_HEIGHT} columns="auto" placeContent="center" placeItems="center" gap="2x">
          <Title level={3} gridArea={false}>
            The chart is too big to display
          </Title>
          <Paragraph>
            There are too many sets of data to display on a single chart. Try to reduce the number
            of dimensions.
          </Paragraph>
        </Grid>
      );
    } else {
      return (
        <ChartContainer
          ref={containerRef}
          style={{
            maxHeight: MAX_HEIGHT,
            height: MAX_HEIGHT,
            overflow,
          }}
        >
          <PlaygroundChartRenderer
            query={query}
            chartType={chartType}
            resultSet={resultSet}
            pivotConfig={pivotConfig}
            chartHeight={MAX_HEIGHT - 20}
          />
        </ChartContainer>
      );
    }
  } else if (!isLoading && isExpanded) {
    return (
      <Grid height={MAX_HEIGHT} columns="auto" placeContent="center" placeItems="center" gap="2x">
        <Title level={3} gridArea={false}>
          No results available
        </Title>
        <Paragraph>Compose and run a query to see the results.</Paragraph>
      </Grid>
    );
  }

  return null;
}
