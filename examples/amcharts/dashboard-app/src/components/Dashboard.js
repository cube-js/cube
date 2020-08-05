import React, { useState, useEffect } from 'react';
import { AutoComplete, Row, Col } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import LineChart from './LineChart';
import Bubble from './Bubble';
import Map from './Map';
import Heatmap from './Heatmap';
import Table from './Table';
import moment from 'moment';

export default () => {
  const [msgByDate, setMsgByDate] = useState([0, 0]);
  const { resultSet: msg } = useCubeQuery({
    measures: ['Messages.count'],
    timeDimensions: [
      {
        dimension: 'Messages.date',
        granularity: 'day',
        dateRange: 'last year',
      },
    ],
  });

  useEffect(() => {
    if (msg) {
      let temp = [];
      msg.tablePivot().map((item) => {
        temp.push({
          date: new Date(item['Messages.date.day']),
          month: moment(item['Messages.date.day']).format('MMM'),
          weekday: moment(item['Messages.date.day']).format('dddd'),
          value: parseInt(item['Messages.count']),
        });
      });
      setMsgByDate(temp);
    }
  }, [msg]);

  const [msgByHour, setMsgByHour] = useState([0, 0]);
  const { resultSet: msgHour } = useCubeQuery({
    measures: ['Messages.count'],
    timeDimensions: [
      {
        dimension: 'Messages.date_time',
        dateRange: 'last week',
        granularity: 'hour',
      },
    ],
  });

  useEffect(() => {
    if (msgHour) {
      let temp = [];
      msgHour.tablePivot().map((item) => {
        temp.push({
          hour: moment(item['Messages.date_time.hour']).format('ha'),
          weekday: moment(item['Messages.date_time.hour']).format('dddd'),
          value: parseInt(item['Messages.count']),
        });
      });
      setMsgByHour(temp);
    }
  }, [msgHour]);

  const [joinsByDate, setJoinsByDate] = useState([0, 0]);
  const { resultSet: joins } = useCubeQuery({
    measures: ['Memberships.sum'],
    timeDimensions: [
      {
        dimension: 'Memberships.date',
        dateRange: 'from 360 days ago to now',
        granularity: 'day',
      },
    ],
  });

  useEffect(() => {
    if (joins) {
      let temp = [];
      joins.tablePivot().map((item) => {
        temp.push({
          date: new Date(item['Memberships.date.day']),
          value: parseInt(item['Memberships.sum']),
        });
      });
      setJoinsByDate(temp);
    }
  }, [joins]);

  const [usersList, setUsersList] = useState({ columns: [], data: [] });
  const { resultSet: users } = useCubeQuery({
    measures: ['Messages.count'],
    dimensions: ['Users.name', 'Users.real_name', 'Users.image'],
  });

  useEffect(() => {
    if (users) {
      let temp = [];
      users.tablePivot().map((item, i) => {
        temp.push({
          key: i,
          name: item['Users.name'],
          real_name: item['Users.real_name'],
          image: item['Users.image'],
          messages: item['Messages.count'],
        });
      });
      setUsersList({
        column: [
          {
            title: 'Image',
            dataIndex: 'image',
            key: 'image',
            width: 50,
            align: 'center',
            render: (url) => <img src={url} width={40} height={40} />,
          },
          {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
          },
          {
            title: 'Real Name',
            dataIndex: 'real_name',
            key: 'real_name',
          },
          {
            title: 'Messages',
            dataIndex: 'messages',
            align: 'center',
            key: 'messages',
          },
        ],
        data: [...temp],
      });
    }
  }, [users]);

  const [channelsList, setChannelsList] = useState({ columns: [], data: [] });
  const { resultSet: channels } = useCubeQuery({
    measures: ['Memberships.count'],
    dimensions: ['Channels.name'],
  });

  useEffect(() => {
    if (channels) {
      let temp = [];
      channels.tablePivot().map((item, i) => {
        temp.push({
          key: i,
          name: `#${item['Channels.name']}`,
          users: item['Memberships.count'],
        });
      });
      setChannelsList({
        column: [
          {
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
          },
          {
            title: 'Users',
            dataIndex: 'users',
            key: 'users',
          },
        ],
        data: [...temp],
      });
    }
  }, [channels]);

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
        <Col sm={24} lg={12}>
          <h2>Messages by date </h2>
          <div className='dashboard__cell'>
            <LineChart data={msgByDate} />
          </div>
        </Col>
        <Col sm={24} lg={12}>
          <h2>Users by date</h2>
          <div className='dashboard__cell'>
            <LineChart data={joinsByDate} />
          </div>
        </Col>
      </Row>

      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={24}>
          <h2>Users by time zone</h2>
          <div className='dashboard__cell'>
            <Map data={joinsByDate} />
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

      <Row className='dashboard__row' gutter={20}>
        <Col sm={24} lg={12}>
          <h2>Channels</h2>
          <div className='dashboard__cell'>
            <Table data={channelsList} />
          </div>
        </Col>
        <Col sm={24} lg={12}>
          <h2>Users</h2>
          <div className='dashboard__cell'>
            <Table data={usersList} />
          </div>
        </Col>
      </Row>
    </React.Fragment>
  );
};
