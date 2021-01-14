import cubejs from '@cubejs-client/core';
import { useCubeQuery } from '@cubejs-client/react';
import { useEffect, useRef, useState } from 'react';
import Muze, { Canvas, Layer } from '@chartshq/react-muze/components';
import { Spin, Row, Col } from 'antd';
import { row } from './App.module.less';
import useSize from './hooks/use-size';

const { REACT_APP_CUBEJS_TOKEN, REACT_APP_API_URL } = process.env;

const CUBE_QUERY = {
  measures: ['Orders.count'],
  dimensions: ['ProductCategories.name', 'Users.gender', 'Users.city'],
  timeDimensions: [
    {
      dimension: 'Orders.createdAt',
      dateRange: ['2019-01-01', '2019-12-31'],
      granularity: 'month',
    },
  ],
};

const cubejsApi = cubejs(REACT_APP_CUBEJS_TOKEN, { apiUrl: REACT_APP_API_URL });

function generateSchema({ dimensions, measures, timeDimensions }) {
  const muzeDimensions = Object.entries(dimensions).map(
    ([name, { title: displayName }]) => ({
      name,
      displayName,
      type: 'dimension',
    })
  );

  const muzeMeasures = Object.entries(measures).map(
    ([name, { title: displayName }]) => ({
      name,
      displayName,
      type: 'measure',
    })
  );

  const muzeTimeDimensions = Object.entries(timeDimensions).map(
    ([name, { title: displayName }]) => ({
      name,
      displayName,
      type: 'dimension',
      subtype: 'temporal',
      format: '%Y-%m-%dT%H:%M:%S',
    })
  );

  return muzeTimeDimensions.concat(muzeMeasures).concat(muzeDimensions);
}

function App() {
  const rowRef = useRef(null);
  const { width, height } = useSize(rowRef);
  const [dataModel, setDataModel] = useState();
  const { resultSet } = useCubeQuery(CUBE_QUERY, { cubejsApi });

  useEffect(() => {
    let dataModel;

    (async () => {
      if (resultSet != null) {
        const data = resultSet.tablePivot();
        const schema = generateSchema(resultSet.annotation());

        const DataModel = await Muze.DataModel.onReady();
        const formattedData = await DataModel.loadData(data, schema);

        dataModel = new DataModel(formattedData);

        setDataModel(dataModel);
      }
    })();

    return () => dataModel != null && dataModel.dispose();
  }, [resultSet]);

  return (
    <Row ref={rowRef} className={row} justify="center" align="middle">
      <Col>
        {dataModel == null ? (
          <Spin size="large" />
        ) : (
          <Muze data={dataModel}>
            <Canvas
              width={width}
              height={height}
              columns={['ProductCategories.name', 'Orders.createdAt.month']}
              rows={['Orders.count', 'Users.city']}
              color={'Users.gender'}
              title="Orders over Time"
              subtitle="Across every category in all cities"
            >
              <Layer mark="bar"></Layer>
            </Canvas>
          </Muze>
        )}
      </Col>
    </Row>
  );
}

export default App;
