import BarChart from '../components/BarChart';
import LoadingIndicator from '../components/LoadingIndicator';
import jwt from 'jsonwebtoken';

const isEmpty = (obj) => Object.keys(obj).length === 0;

export const years = [
  { id: 1, year: 1988 },
  { id: 2, year: 1989 },
  { id: 3, year: 1990 },
  { id: 4, year: 1991 },
  { id: 5, year: 1992 },
  { id: 6, year: 1993 },
  { id: 7, year: 1994 },
  { id: 8, year: 1995 },
  { id: 9, year: 1996 },
  { id: 10, year: 1997 },
  { id: 11, year: 1998 },
  { id: 12, year: 1999 },
  { id: 13, year: 2000 },
  { id: 14, year: 2001 },
  { id: 15, year: 2002 },
  { id: 16, year: 2003 },
  { id: 17, year: 2004 },
  { id: 18, year: 2005 },
  { id: 19, year: 2006 },
  { id: 20, year: 2007 },
  { id: 21, year: 2008 },
  { id: 22, year: 2009 },
  { id: 23, year: 2010 },
  { id: 24, year: 2011 },
  { id: 25, year: 2012 },
  { id: 26, year: 2013 },
  { id: 27, year: 2014 },
  { id: 28, year: 2015 },
  { id: 29, year: 2016 },
  { id: 30, year: 2017 },
  { id: 31, year: 2018 },
  { id: 32, year: 2019 },
  { id: 33, year: 2020 },
  { id: 34, year: 2021 },
];

export const defaultJwtSecret = '1c7548fdc11622f711fd0113139feefc4cbd88826d3107b29b4950b0b1df159c'
export const defaultYearId = 1
/** OSS Cube */
// const defaultApiUrl = 'http://localhost:4000/cubejs-api/v1'
/** Cube Cloud */
export const defaultApiUrl = 'https://blue-stork.aws-us-east-1.cubecloudapp.dev/dev-mode/demo1/cubejs-api/v1'
const jwtSecret = defaultJwtSecret
export const token = jwt.sign({
  exp: 5000000000,
}, jwtSecret);

export const jsonQuery = ({ year, dataSource }) => ({
  measures: [ `Ontime_${dataSource}.avgDepDelayGreaterThanTenMinutesPercentage` ],
  timeDimensions: [ {
    dimension: `Ontime_${dataSource}.flightdate`,
    granularity: 'month',
  } ],
  filters: [ {
    member: `Ontime_${dataSource}.year`,
    operator: 'equals',
    values: [ `${year}` ]
  } ],
})

export function DisplayBarChart({ chartData }) {
  if (!chartData || isEmpty(chartData)) {
    return <LoadingIndicator />;
  }
  
  return (
    <BarChart
      data={chartData}
    />
  );
}