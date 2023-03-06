import { DB_CAST } from './CAST';

export const sql = (type) => {
  const { SELECT_PREFIX, SELECT_SUFFIX } = DB_CAST[type];
  const select = `
    select 'AH-10465' as customer_id, 'Customer 1' as customer_name union all
    select 'AJ-10780' as customer_id, 'Customer 2' as customer_name union all
    select 'AS-10225' as customer_id, 'Customer 3' as customer_name union all
    select 'AW-10840' as customer_id, 'Customer 4' as customer_name union all
    select 'BB-11545' as customer_id, 'Customer 5' as customer_name union all
    select 'BF-11020' as customer_id, 'Customer 6' as customer_name union all
    select 'BF-11170' as customer_id, 'Customer 7' as customer_name union all
    select 'BM-11650' as customer_id, 'Customer 8' as customer_name union all
    select 'BS-11380' as customer_id, 'Customer 9' as customer_name union all
    select 'BS-11755' as customer_id, 'Customer 10' as customer_name union all
    select 'CA-12775' as customer_id, 'Customer 11' as customer_name union all
    select 'CC-12475' as customer_id, 'Customer 12' as customer_name union all
    select 'CD-12280' as customer_id, 'Customer 13' as customer_name union all
    select 'CS-12355' as customer_id, 'Customer 14' as customer_name union all
    select 'DB-13405' as customer_id, 'Customer 15' as customer_name union all
    select 'DG-13300' as customer_id, 'Customer 16' as customer_name union all
    select 'DW-13480' as customer_id, 'Customer 17' as customer_name union all
    select 'EM-14140' as customer_id, 'Customer 18' as customer_name union all
    select 'GA-14725' as customer_id, 'Customer 19' as customer_name union all
    select 'GZ-14470' as customer_id, 'Customer 20' as customer_name union all
    select 'HH-15010' as customer_id, 'Customer 21' as customer_name union all
    select 'HK-14890' as customer_id, 'Customer 22' as customer_name union all
    select 'JH-15430' as customer_id, 'Customer 23' as customer_name union all
    select 'JO-15550' as customer_id, 'Customer 24' as customer_name union all
    select 'JS-16030' as customer_id, 'Customer 25' as customer_name union all
    select 'JW-15220' as customer_id, 'Customer 26' as customer_name union all
    select 'KL-16555' as customer_id, 'Customer 27' as customer_name union all
    select 'KN-16705' as customer_id, 'Customer 28' as customer_name union all
    select 'LC-17050' as customer_id, 'Customer 29' as customer_name union all
    select 'LR-16915' as customer_id, 'Customer 30' as customer_name union all
    select 'MC-17605' as customer_id, 'Customer 31' as customer_name union all
    select 'MG-17650' as customer_id, 'Customer 32' as customer_name union all
    select 'ML-17755' as customer_id, 'Customer 33' as customer_name union all
    select 'MM-18280' as customer_id, 'Customer 34' as customer_name union all
    select 'NP-18670' as customer_id, 'Customer 35' as customer_name union all
    select 'PF-19165' as customer_id, 'Customer 36' as customer_name union all
    select 'SB-20185' as customer_id, 'Customer 37' as customer_name union all
    select 'SS-20140' as customer_id, 'Customer 38' as customer_name union all
    select 'TB-21175' as customer_id, 'Customer 39' as customer_name union all
    select 'TS-21205' as customer_id, 'Customer 40' as customer_name union all
    select 'WB-21850' as customer_id, 'Customer 41' as customer_name
  `;
  return SELECT_PREFIX + select + SELECT_SUFFIX;
};
