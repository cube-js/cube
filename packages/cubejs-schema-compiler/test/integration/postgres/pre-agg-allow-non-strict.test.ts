import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';

const getSql = () => `
  select 3060 as row_id, 'CA-2017-131492' as order_id, to_date('2020-10-19', 'YYYY-MM-DD') as order_date, 'HH-15010' as customer_id, 'San Francisco' as city, 'Furniture' as category, 'Tables' as sub_category, 'Anderson Hickey Conga Table Tops & Accessories' as product_name, 24.36800 as sales, 2 as quantity, 0.20000 as discount, -3.35060 as profit union all
  select 523 as row_id, 'CA-2017-145142' as order_id, to_date('2020-01-23', 'YYYY-MM-DD') as order_date, 'MC-17605' as customer_id, 'Detroit' as city, 'Furniture' as category, 'Tables' as sub_category, 'Balt Solid Wood Rectangular Table' as product_name, 210.98000 as sales, 2 as quantity, 0.00000 as discount, 21.09800 as profit union all
  select 9584 as row_id, 'CA-2017-116127' as order_id, to_date('2020-06-25', 'YYYY-MM-DD') as order_date, 'SB-20185' as customer_id, 'New York City' as city, 'Furniture' as category, 'Bookcases' as sub_category, 'DMI Eclipse Executive Suite Bookcases' as product_name, 400.78400 as sales, 1 as quantity, 0.20000 as discount, -5.00980 as profit union all
  select 8425 as row_id, 'CA-2017-150091' as order_id, to_date('2020-10-12', 'YYYY-MM-DD') as order_date, 'NP-18670' as customer_id, 'Lakewood' as city, 'Furniture' as category, 'Bookcases' as sub_category, 'Global Adaptabilites Bookcase, Cherry/Storm Gray Finish' as product_name, 2154.90000 as sales, 5 as quantity, 0.00000 as discount, 129.29400 as profit union all
  select 2655 as row_id, 'CA-2017-112515' as order_id, to_date('2020-09-17', 'YYYY-MM-DD') as order_date, 'AS-10225' as customer_id, 'Provo' as city, 'Furniture' as category, 'Bookcases' as sub_category, 'Global Adaptabilites Bookcase, Cherry/Storm Gray Finish' as product_name, 1292.94000 as sales, 3 as quantity, 0.00000 as discount, 77.57640 as profit union all
  select 2952 as row_id, 'CA-2017-134915' as order_id, to_date('2020-11-12', 'YYYY-MM-DD') as order_date, 'EM-14140' as customer_id, 'Glendale' as city, 'Furniture' as category, 'Chairs' as sub_category, 'Harbour Creations 67200 Series Stacking Chairs' as product_name, 113.88800 as sales, 2 as quantity, 0.20000 as discount, 9.96520 as profit union all
  select 9473 as row_id, 'CA-2017-102925' as order_id, to_date('2020-11-05', 'YYYY-MM-DD') as order_date, 'CD-12280' as customer_id, 'New York City' as city, 'Furniture' as category, 'Chairs' as sub_category, 'Harbour Creations 67200 Series Stacking Chairs' as product_name, 128.12400 as sales, 2 as quantity, 0.10000 as discount, 24.20120 as profit union all
  select 5220 as row_id, 'CA-2017-145653' as order_id, to_date('2020-09-01', 'YYYY-MM-DD') as order_date, 'CA-12775' as customer_id, 'Detroit' as city, 'Furniture' as category, 'Chairs' as sub_category, 'Harbour Creations 67200 Series Stacking Chairs' as product_name, 498.26000 as sales, 7 as quantity, 0.00000 as discount, 134.53020 as profit union all
  select 4031 as row_id, 'CA-2017-124296' as order_id, to_date('2020-12-24', 'YYYY-MM-DD') as order_date, 'CS-12355' as customer_id, 'Lafayette' as city, 'Furniture' as category, 'Chairs' as sub_category, 'Iceberg Nesting Folding Chair, 19w x 6d x 43h' as product_name, 232.88000 as sales, 4 as quantity, 0.00000 as discount, 60.54880 as profit union all
  select 8621 as row_id, 'US-2017-119319' as order_id, to_date('2020-11-06', 'YYYY-MM-DD') as order_date, 'LC-17050' as customer_id, 'Dallas' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Linden 10 Round Wall Clock, Black' as product_name, 30.56000 as sales, 5 as quantity, 0.60000 as discount, -19.86400 as profit union all
  select 3059 as row_id, 'CA-2017-131492' as order_id, to_date('2020-10-19', 'YYYY-MM-DD') as order_date, 'HH-15010' as customer_id, 'San Francisco' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Linden 10 Round Wall Clock, Black' as product_name, 30.56000 as sales, 2 as quantity, 0.00000 as discount, 10.39040 as profit union all
  select 7425 as row_id, 'CA-2017-135069' as order_id, to_date('2020-04-10', 'YYYY-MM-DD') as order_date, 'BS-11755' as customer_id, 'Philadelphia' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Linden 10 Round Wall Clock, Black' as product_name, 36.67200 as sales, 3 as quantity, 0.20000 as discount, 6.41760 as profit union all
  select 849 as row_id, 'CA-2017-107503' as order_id, to_date('2020-01-01', 'YYYY-MM-DD') as order_date, 'GA-14725' as customer_id, 'Lorain' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Linden 10 Round Wall Clock, Black' as product_name, 48.89600 as sales, 4 as quantity, 0.20000 as discount, 8.55680 as profit union all
  select 6205 as row_id, 'CA-2017-145660' as order_id, to_date('2020-12-01', 'YYYY-MM-DD') as order_date, 'MG-17650' as customer_id, 'Marion' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Magna Visual Magnetic Picture Hangers' as product_name, 7.71200 as sales, 2 as quantity, 0.20000 as discount, 1.73520 as profit union all
  select 1494 as row_id, 'CA-2017-139661' as order_id, to_date('2020-10-30', 'YYYY-MM-DD') as order_date, 'JW-15220' as customer_id, 'Vancouver' as city, 'Furniture' as category, 'Furnishings' as sub_category, 'Magna Visual Magnetic Picture Hangers' as product_name, 9.64000 as sales, 2 as quantity, 0.00000 as discount, 3.66320 as profit union all
  select 3934 as row_id, 'CA-2017-123001' as order_id, to_date('2020-09-02', 'YYYY-MM-DD') as order_date, 'AW-10840' as customer_id, 'Bakersfield' as city, 'Office Supplies' as category, 'Art' as sub_category, 'OIC #2 Pencils, Medium Soft' as product_name, 9.40000 as sales, 5 as quantity, 0.00000 as discount, 2.72600 as profit union all
  select 3448 as row_id, 'CA-2017-102554' as order_id, to_date('2020-06-11', 'YYYY-MM-DD') as order_date, 'KN-16705' as customer_id, 'Auburn' as city, 'Office Supplies' as category, 'Art' as sub_category, 'OIC #2 Pencils, Medium Soft' as product_name, 3.76000 as sales, 2 as quantity, 0.00000 as discount, 1.09040 as profit union all
  select 6459 as row_id, 'US-2017-133361' as order_id, to_date('2020-05-14', 'YYYY-MM-DD') as order_date, 'AJ-10780' as customer_id, 'Baltimore' as city, 'Office Supplies' as category, 'Art' as sub_category, 'OIC #2 Pencils, Medium Soft' as product_name, 3.76000 as sales, 2 as quantity, 0.00000 as discount, 1.09040 as profit union all
  select 6272 as row_id, 'CA-2017-102379' as order_id, to_date('2020-12-02', 'YYYY-MM-DD') as order_date, 'BB-11545' as customer_id, 'Oakland' as city, 'Office Supplies' as category, 'Art' as sub_category, 'Panasonic KP-380BK Classic Electric Pencil Sharpener' as product_name, 179.90000 as sales, 5 as quantity, 0.00000 as discount, 44.97500 as profit union all
  select 9619 as row_id, 'CA-2017-160633' as order_id, to_date('2020-11-16', 'YYYY-MM-DD') as order_date, 'BS-11380' as customer_id, 'Bowling' as city, 'Office Supplies' as category, 'Art' as sub_category, 'Panasonic KP-380BK Classic Electric Pencil Sharpener' as product_name, 86.35200 as sales, 3 as quantity, 0.20000 as discount, 5.39700 as profit union all
  select 1013 as row_id, 'CA-2017-118437' as order_id, to_date('2020-06-17', 'YYYY-MM-DD') as order_date, 'PF-19165' as customer_id, 'Olympia' as city, 'Office Supplies' as category, 'Storage' as sub_category, 'Project Tote Personal File' as product_name, 14.03000 as sales, 1 as quantity, 0.00000 as discount, 4.06870 as profit union all
  select 4012 as row_id, 'CA-2017-100811' as order_id, to_date('2020-11-21', 'YYYY-MM-DD') as order_date, 'CC-12475' as customer_id, 'Philadelphia' as city, 'Office Supplies' as category, 'Storage' as sub_category, 'Recycled Eldon Regeneration Jumbo File' as product_name, 39.29600 as sales, 4 as quantity, 0.20000 as discount, 3.92960 as profit union all
  select 2595 as row_id, 'CA-2017-149048' as order_id, to_date('2020-05-13', 'YYYY-MM-DD') as order_date, 'BM-11650' as customer_id, 'Columbus' as city, 'Office Supplies' as category, 'Envelopes' as sub_category, 'Tyvek Side-Opening Peel & Seel Expanding Envelopes' as product_name, 180.96000 as sales, 2 as quantity, 0.00000 as discount, 81.43200 as profit union all
  select 2329 as row_id, 'CA-2017-138422' as order_id, to_date('2020-09-23', 'YYYY-MM-DD') as order_date, 'KN-16705' as customer_id, 'Columbus' as city, 'Office Supplies' as category, 'Envelopes' as sub_category, 'Wausau Papers Astrobrights Colored Envelopes' as product_name, 14.35200 as sales, 3 as quantity, 0.20000 as discount, 5.20260 as profit union all
  select 4227 as row_id, 'CA-2017-120327' as order_id, to_date('2020-11-11', 'YYYY-MM-DD') as order_date, 'WB-21850' as customer_id, 'Columbus' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Vinyl Coated Wire Paper Clips in Organizer Box, 800/Box' as product_name, 45.92000 as sales, 4 as quantity, 0.00000 as discount, 21.58240 as profit union all
  select 6651 as row_id, 'US-2017-124779' as order_id, to_date('2020-09-08', 'YYYY-MM-DD') as order_date, 'BF-11020' as customer_id, 'Arlington' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Vinyl Coated Wire Paper Clips in Organizer Box, 800/Box' as product_name, 45.92000 as sales, 5 as quantity, 0.20000 as discount, 15.49800 as profit union all
  select 8673 as row_id, 'CA-2017-163265' as order_id, to_date('2020-02-16', 'YYYY-MM-DD') as order_date, 'JS-16030' as customer_id, 'Decatur' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Vinyl Coated Wire Paper Clips in Organizer Box, 800/Box' as product_name, 18.36800 as sales, 2 as quantity, 0.20000 as discount, 6.19920 as profit union all
  select 1995 as row_id, 'CA-2017-133648' as order_id, to_date('2020-06-25', 'YYYY-MM-DD') as order_date, 'ML-17755' as customer_id, 'Columbus' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Plymouth Boxed Rubber Bands by Plymouth' as product_name, 11.30400 as sales, 3 as quantity, 0.20000 as discount, -2.11950 as profit union all
  select 7310 as row_id, 'CA-2017-112172' as order_id, to_date('2020-06-10', 'YYYY-MM-DD') as order_date, 'MM-18280' as customer_id, 'New York City' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Plymouth Boxed Rubber Bands by Plymouth' as product_name, 14.13000 as sales, 3 as quantity, 0.00000 as discount, 0.70650 as profit union all
  select 3717 as row_id, 'CA-2017-144568' as order_id, to_date('2020-05-29', 'YYYY-MM-DD') as order_date, 'JO-15550' as customer_id, 'Omaha' as city, 'Office Supplies' as category, 'Fasteners' as sub_category, 'Plymouth Boxed Rubber Bands by Plymouth' as product_name, 23.55000 as sales, 5 as quantity, 0.00000 as discount, 1.17750 as profit union all
  select 4882 as row_id, 'CA-2017-143567' as order_id, to_date('2020-11-02', 'YYYY-MM-DD') as order_date, 'TB-21175' as customer_id, 'Columbus' as city, 'Technology' as category, 'Accessories' as sub_category, 'Logitech diNovo Edge Keyboard' as product_name, 2249.91000 as sales, 9 as quantity, 0.00000 as discount, 517.47930 as profit union all
  select 5277 as row_id, 'CA-2017-147333' as order_id, to_date('2020-12-14', 'YYYY-MM-DD') as order_date, 'KL-16555' as customer_id, 'Columbus' as city, 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.0' as product_name, 44.75000 as sales, 5 as quantity, 0.00000 as discount, 8.50250 as profit union all
  select 6125 as row_id, 'CA-2017-145772' as order_id, to_date('2020-06-03', 'YYYY-MM-DD') as order_date, 'SS-20140' as customer_id, 'Los Angeles' as city, 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.1' as product_name, 44.75000 as sales, 5 as quantity, 0.00000 as discount, 8.50250 as profit union all
  select 2455 as row_id, 'CA-2017-140949' as order_id, to_date('2020-03-17', 'YYYY-MM-DD') as order_date, 'DB-13405' as customer_id, 'New York City' as city, 'Technology' as category, 'Accessories' as sub_category, 'Kingston Digital DataTraveler 16GB USB 2.2' as product_name, 71.60000 as sales, 8 as quantity, 0.00000 as discount, 13.60400 as profit union all
  select 2661 as row_id, 'CA-2017-123372' as order_id, to_date('2020-11-28', 'YYYY-MM-DD') as order_date, 'DG-13300' as customer_id, 'Columbus' as city, 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 5' as product_name, 1979.89000 as sales, 11 as quantity, 0.00000 as discount, 494.97250 as profit union all
  select 3083 as row_id, 'US-2017-132297' as order_id, to_date('2020-05-27', 'YYYY-MM-DD') as order_date, 'DW-13480' as customer_id, 'Louisville' as city, 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 6' as product_name, 539.97000 as sales, 3 as quantity, 0.00000 as discount, 134.99250 as profit union all
  select 4161 as row_id, 'CA-2017-115546' as order_id, to_date('2020-05-14', 'YYYY-MM-DD') as order_date, 'AH-10465' as customer_id, 'New York City' as city, 'Technology' as category, 'Phones' as sub_category, 'Google Nexus 7' as product_name, 539.97000 as sales, 3 as quantity, 0.00000 as discount, 134.99250 as profit union all
  select 8697 as row_id, 'CA-2017-119284' as order_id, to_date('2020-06-15', 'YYYY-MM-DD') as order_date, 'TS-21205' as customer_id, 'Columbus' as city, 'Technology' as category, 'Phones' as sub_category, 'HTC One' as product_name, 239.97600 as sales, 3 as quantity, 0.20000 as discount, 26.99730 as profit union all
  select 7698 as row_id, 'CA-2017-151799' as order_id, to_date('2020-12-14', 'YYYY-MM-DD') as order_date, 'BF-11170' as customer_id, 'Columbus' as city, 'Technology' as category, 'Copiers' as sub_category, 'Canon PC1080F Personal Copier' as product_name, 1199.98000 as sales, 2 as quantity, 0.00000 as discount, 467.99220 as profit union all
  select 7174 as row_id, 'US-2017-141677' as order_id, to_date('2020-03-26', 'YYYY-MM-DD') as order_date, 'HK-14890' as customer_id, 'Houston' as city, 'Technology' as category, 'Copiers' as sub_category, 'Canon PC1080F Personal Copier' as product_name, 2399.96000 as sales, 5 as quantity, 0.20000 as discount, 569.99050 as profit union all
  select 9618 as row_id, 'CA-2017-160633' as order_id, to_date('2020-11-16', 'YYYY-MM-DD') as order_date, 'BS-11380' as customer_id, 'Columbus' as city, 'Technology' as category, 'Copiers' as sub_category, 'Hewlett Packard 610 Color Digital Copier / Printer' as product_name, 899.98200 as sales, 3 as quantity, 0.40000 as discount, 74.99850 as profit union all
  select 8958 as row_id, 'CA-2017-105620' as order_id, to_date('2020-12-25', 'YYYY-MM-DD') as order_date, 'JH-15430' as customer_id, 'Columbus' as city, 'Technology' as category, 'Machines' as sub_category, 'Lexmark 20R1285 X6650 Wireless All-in-One Printer' as product_name, 120.00000 as sales, 2 as quantity, 0.50000 as discount, -7.20000 as profit union all
  select 8878 as row_id, 'CA-2017-126928' as order_id, to_date('2020-09-17', 'YYYY-MM-DD') as order_date, 'GZ-14470' as customer_id, 'Morristown' as city, 'Technology' as category, 'Machines' as sub_category, 'Lexmark 20R1285 X6650 Wireless All-in-One Printer' as product_name, 480.00000 as sales, 4 as quantity, 0.00000 as discount, 225.60000 as profit union all
  select 7293 as row_id, 'CA-2017-109183' as order_id, to_date('2020-12-04', 'YYYY-MM-DD') as order_date, 'LR-16915' as customer_id, 'Columbus' as city, 'Technology' as category, 'Machines' as sub_category, 'Okidata C610n Printer' as product_name, 649.00000 as sales, 2 as quantity, 0.50000 as discount, -272.58000 as profit
`;

const getCube = (
  allowMonth: boolean,
  allowDay: boolean,
  allowHour: boolean,
): string => `
  cube(\`cube\`, {
    sql: \`${getSql()}\`,
    preAggregations: {
      MonthlyData: {
        timeDimension: CUBE.orderDate,
        granularity: \`month\`,
        allowNonStrictDateRangeMatch: ${allowMonth},
        dimensions: [
          CUBE.city,
        ],
        measures: [
          CUBE.totalQuantity,
          CUBE.totalProfit,
        ],
        refreshKey: {
          every: \`1 day\`,
        },
        external: false,
      },
      DailyData: {
        timeDimension: CUBE.orderDate,
        granularity: \`day\`,
        allowNonStrictDateRangeMatch: ${allowDay},
        dimensions: [
          CUBE.city,
        ],
        measures: [
          CUBE.totalQuantity,
          CUBE.totalProfit,
        ],
        refreshKey: {
          every: \`1 day\`,
        },
        external: false,
      },
      HourlyData: {
        timeDimension: CUBE.orderDate,
        granularity: \`hour\`,
        allowNonStrictDateRangeMatch: ${allowHour},
        dimensions: [
          CUBE.city,
        ],
        measures: [
          CUBE.totalQuantity,
          CUBE.totalProfit,
        ],
        refreshKey: {
          every: \`1 day\`,
        },
        external: false,
      }
    },
    measures: {
      totalQuantity: {
        sql: 'quantity',
        type: 'sum',
      },
      totalProfit: {
        sql: 'profit',
        type: 'sum',
      },
    },
    dimensions: {
      orderDate: {
        sql: 'order_date',
        type: 'time',
      },
      city: {
        sql: 'city',
        type: 'string',
      },
    },
  });
`;

const getQueries = (compiler, joinGraph, cubeEvaluator) => ([
  // month query
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-31 23:59:59.999'],
      granularity: 'month'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // month to day
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-31 23:59:59.999'],
      granularity: 'week'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // day
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-30 23:59:59.999'],
      granularity: 'week'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // hour
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-30 22:59:59.999'],
      granularity: 'week'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // minute
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-30 22:50:59.999'],
      granularity: 'week'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // second
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-30 22:50:50.999'],
      granularity: 'week'
    }],
    timezone: 'America/Los_Angeles',
  }),
  // no granularity
  new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
    measures: ['cube.totalQuantity', 'cube.totalProfit'],
    dimensions: ['cube.city'],
    timeDimensions: [{
      dimension: 'cube.orderDate',
      dateRange: ['2020-01-01 00:00:00.000', '2020-03-30 22:50:50.999'],
    }],
    timezone: 'America/Los_Angeles',
  }),
]);

describe(
  'PreAggregations with the time dimension and the `allowNonStrictDateRangeMatch` flag',
  () => {
    describe('The `MonthlyData` pre-aggregation with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(true, false, false));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('minute query with the `week` granularity does not match any pre-aggregation', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('second query with the `week` granularity does not match any pre-aggregation', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('query with no granularity match MonthlyData', async () => {
        await compiler.compile();
        const [,,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });
    });

    describe('The `DailyData` pre-aggregation with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(false, true, false));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('minute query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('second query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('query with no granularity match MonthlyData', async () => {
        await compiler.compile();
        const [,,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });
    });

    describe('The `HourlyData` pre-aggregation with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(false, false, true));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('minute query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('second query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('query with no granularity match HourlyData', async () => {
        await compiler.compile();
        const [,,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });
    });

    describe('`MonthlyData` and `DailyData` pre-aggregations with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(true, true, false));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('minute query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('second query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });
    });

    describe('`MonthlyData` and `HourlyData` pre-aggregations with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(true, false, true));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('minute query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });

      it('second query with the `week` granularity match `HourlyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toBeGreaterThanOrEqual(0);
      });
    });

    describe('`DailyData` and `HourlyData` pre-aggregations with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(false, true, true));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('minute query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('second query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });
    });

    describe('All pre-aggregations with the `allowNonStrictDateRangeMatch` enabled', () => {
      jest.setTimeout(200000);

      const { compiler, joinGraph, cubeEvaluator } =
        prepareJsCompiler(getCube(true, true, true));

      it('month query with the `month` granularity match `MonthlyData`', async () => {
        await compiler.compile();
        const [request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__daily_data')).toEqual(-1);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('month query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('day query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('hour query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('minute query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });

      it('second query with the `week` granularity match `DailyData`', async () => {
        await compiler.compile();
        const [,,,,, request] = getQueries(compiler, joinGraph, cubeEvaluator);
        const [query] = request.buildSqlAndParams();
        expect(query.indexOf('cube__monthly_data')).toEqual(-1);
        expect(query.indexOf('cube__daily_data')).toBeGreaterThanOrEqual(0);
        expect(query.indexOf('cube__hourly_data')).toEqual(-1);
      });
    });
  }
);
