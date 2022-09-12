import LineChart from '../components/LineChart';
import LoadingIndicator from '../components/LoadingIndicator';

const groupByKey = (arr, key, {omitKey=false}) =>
  arr.reduce((hash, {[key]:value, ...rest}) => { 
    return {
      ...hash,
      [value]:( hash[value] || [] ).concat(omitKey ? {...rest} : {[key]:value, ...rest})
    }
  }, {});

export const availableStepRanges = [
  { id: 1, start: 1, end: 50 },
  { id: 2, start: 50, end: 100 },
  { id: 3, start: 100, end: 150 },
  { id: 4, start: 150, end: 200 },
  { id: 5, start: 200, end: 250 },
  { id: 6, start: 250, end: 300 },
  { id: 7, start: 300, end: 350 },
  { id: 8, start: 400, end: 450 },
  { id: 9, start: 450, end: 500 },
  { id: 10, start: 500, end: 550 },
  { id: 11, start: 550, end: 600 },
  { id: 12, start: 600, end: 650 },
  { id: 13, start: 650, end: 700 },
  { id: 14, start: 700, end: 750 },
];

export const defaultStepSelection = 1;
export const defaultIsFraudSelection = 1;

export const randomIntFromInterval = (min, max) => { 
  return Math.floor(Math.random() * (max - min + 1) + min)
}

export function range(start, end) {
  const arr = [...Array(end - start + 1).keys()].map(x => x + start)
  return arr;
}

export function tablePivotCube(data) {
  const flattened = data.cube.map(i => ({ y: i.fraud.amountSum, x: i.fraud.step, type: i.fraud.type }));
  const groupedHash = groupByKey(flattened, 'type', {omitKey:true});
  const reduced = Object.keys(groupedHash).reduce((accumulator, iterator, key) => {
    accumulator.push({
      id: iterator,
      data: groupedHash[iterator],
    });

    return accumulator;
  }, []);

  return reduced;
}

export function tablePivotHasura(data) {
  const flattened = data.map(i => ({ y: i.fraud__amount_sum, x: i.fraud__step, type: i.fraud__type }));
  const groupedHash = groupByKey(flattened, 'type', {omitKey:true});
  const reduced = Object.keys(groupedHash).reduce((accumulator, iterator, key) => {
    accumulator.push({
      id: iterator,
      data: groupedHash[iterator],
    });

    return accumulator;
  }, []);

  return reduced;
}

export function DisplayFraudAmountSum({ error, loading, chartData }) {
  if (loading) return <LoadingIndicator />;

  if (error) {
    console.error(error);
    return <p>Error :( </p>;
  }

  return (
    <LineChart
      data={chartData}
    />
  );
}