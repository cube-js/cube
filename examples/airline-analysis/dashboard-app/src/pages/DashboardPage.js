import { Col, Row } from 'antd';
import React from 'react';
import ChartRenderer from '../components/ChartRenderer';
import Dashboard from '../components/Dashboard';
import DashboardItem from '../components/DashboardItem';

const DashboardPage = () => {  
  const dashboardItem = <div>
    <Row>
        <Col span={24}>
          <DashboardItem title={
            <span style={{fontWeight: 'bold', fontSize: '1.5rem'}}>
              Flights Delayed by Minutes
            </span>
            }>
            <ChartRenderer chartType={'line'}/>        
          </DashboardItem>
        </Col>
    </Row>
    <Row style={{display: 'flex', justifyContent: 'space-between'}}>
      <Col span={12}>
        <DashboardItem title={
          <span style={{fontWeight: 'bold'}}>
            Total Flights from Airports
          </span>}>
          <ChartRenderer chartType={'area'}/>        
        </DashboardItem>
      </Col>
      <Col span={11}>
        <DashboardItem title={
          <span style={{fontWeight: 'bold'}}>
            Total Airline Carriers per Month
          </span>}>
          <ChartRenderer chartType={'pie'}/>        
        </DashboardItem>
      </Col>
    </Row>
    <Row>
        <Col span={24}>
          <DashboardItem>
            <ChartRenderer chartType={'table'}/>        
          </DashboardItem>
        </Col>
    </Row>
    </div>

  return <Dashboard>
      {dashboardItem}
    </Dashboard>
};

export default DashboardPage;