import React, { useState, useEffect } from 'react';
import { AutoComplete, Row, Col } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import LineChart from './LineChart';
import Bubble from './Bubble';
import Heatmap from './Heatmap';
import moment from 'moment';

export default () => {
  /*const [msgByDate, setMsgByDate] = useState([0, 0]);
  const { resultSet: msg } = useCubeQuery({
    measures: ['Data.count'],
    timeDimensions: [
      {
        dimension: 'Data.ts',
        granularity: 'day',
        dateRange: 'last year',
      },
    ],
  });

  const [msgByHour, setMsgByHour] = useState([0, 0]);
  const { resultSet: msgHour } = useCubeQuery({
    measures: ['Data.count'],
    timeDimensions: [
      {
        dimension: 'Data.ts',
        dateRange: 'last week',
        granularity: 'hour',
      },
    ],
  });

  const [joinsByDate, setJoinsByDate] = useState([0, 0]);
  const { resultSet: joins } = useCubeQuery({
    measures: ['Data.count'],
    //dimensions: ['Data.subtype'],
    renewQuery: true,
    filters: [
      {
        member: 'Data.subtype',
        operator: 'equals',
        values: ['channel_join'],
      },
    ],
    timeDimensions: [
      {
        dimension: 'Data.ts',
        granularity: 'day',
      },
    ],
  });

  useEffect(() => {
    if (msg) {
      let temp = [];
      msg.tablePivot().map((item) => {
        temp.push({
          date: new Date(item['Data.ts.day']),
          month: moment(item['Data.ts.day']).format('MMM'),
          weekday: moment(item['Data.ts.day']).format('dddd'),
          value: parseInt(item['Data.count']),
        });
      });
      setMsgByDate(temp);
    }
  }, [msg]);

  useEffect(() => {
    if (msgHour) {
      let temp = [];
      msgHour.tablePivot().map((item) => {
        temp.push({
          hour: moment(item['Data.ts.hour']).format('ha'),
          weekday: moment(item['Data.ts.hour']).format('dddd'),
          value: parseInt(item['Data.count']),
        });
      });
      setMsgByHour(temp);
    }
  }, [msgHour]);

  useEffect(() => {
    if (joins) {
      let temp = [];
      joins.tablePivot().map((item) => {
        temp.push({
          date: new Date(item['Data.ts.day']),
          value: parseInt(item['Data.count']),
        });
      });
      setJoinsByDate(temp);
    }
  }, [joins]);

  return (
    <React.Fragment>
      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={24}>
          <div className='examples__buttons'>
            <span className='examples__button'>All activity</span>
            <span className='examples__button'>
              Activity in
              <AutoComplete
                style={{
                  minWidth: 100,
                }}
                placeholder='#general'
              />
            </span>
            <span className='examples__button'>
              Activity by{' '}
              <AutoComplete
                style={{
                  minWidth: 100,
                }}
                placeholder='Igor Lukanin'
              />
            </span>
            <span className='examples__button'>
              Last{' '}
              <AutoComplete
                style={{
                  minWidth: 100,
                }}
                placeholder='30 days'
              />
            </span>
          </div>
        </Col>
      </Row>

      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={24}>
          <h2>Messages by date </h2>
          <div className='dashboard__cell'>
            <LineChart data={msgByDate} />
          </div>
        </Col>
      </Row>

      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={24}>
          <h2>Joins by date</h2>
          <div className='dashboard__cell'>
            <LineChart data={joinsByDate} />
          </div>
        </Col>
      </Row>
      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={12}>
          <h2>Messages by day last year</h2>
          <div className='dashboard__cell'>
            <Heatmap data={msgByDate} />
          </div>
        </Col>
        <Col sm={24} lg={12}>
          <h2>Messages by time last week</h2>
          <div className='dashboard__cell'>
            <Bubble data={msgByHour} />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );*/
  return '<div>123</div>';
};
