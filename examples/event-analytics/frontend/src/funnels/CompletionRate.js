import React from 'react';
import Typography from '@material-ui/core/Typography';

import { QueryRenderer } from '@cubejs-client/react';
import cubejsClient from '../cubejsClient';

const calculateCompletionRate = (resultSet, id) => {
  const data = resultSet.rawData()
  const last = Number(data[data.length - 1][`${id}.conversions`])
  if (last === 0) { return 0 }

  const first = Number(data[0][`${id}.conversions`])

  return Math.round(last/first);
}

const CompletionRate = ({ query, id }) => (
  <>
    <QueryRenderer
      query={query}
      cubejsApi={cubejsClient}
      render={({ resultSet }) => {
        if (resultSet) {
          return (
            <>
              <Typography variant="h6">
                Completion Rate: { calculateCompletionRate(resultSet, id) }%
              </Typography>
              <Typography variant="subtitle1">
                30 days to complete funnel
              </Typography>
            </>
          )
        }
        return null
      }}
    />
  </>
)

export default CompletionRate
