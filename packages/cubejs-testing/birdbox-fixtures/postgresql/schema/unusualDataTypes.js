cube('unusualDataTypes', {
    sql: `SELECT
              1 AS id,
              100 AS amount,
              'new' AS status,
              '{"key": "value1", "number": 42}'::json AS json_column,
              '{"key": "value1", "number": 42}'::jsonb AS jsonb_column,
              ARRAY[1, 2, 3] AS array_column,
              '11:22:33:44:55:66'::macaddr AS mac_address,
              '192.168.0.1'::inet AS inet_column,
              '192.168.0.0/24'::cidr AS cidr_column,
              't'::boolean AS boolean_column,
              'Hello, world!'::text AS text_column,
              '1.0, 1.0'::point AS point_column,
              '11111111'::bit(8) AS bit_column,
              '<root><child>data</child></root>'::xml AS xml_column
          UNION ALL
          SELECT
              2 AS id,
              200 AS amount,
              'new' AS status,
              '{"key": "value2", "number": 84}'::json AS json_column,
              '{"key": "value2", "number": 84}'::jsonb AS jsonb_column,
              ARRAY[4, 5, 6] AS array_column,
              '00:11:22:33:44:55'::macaddr AS mac_address,
              '192.168.0.2'::inet AS inet_column,
              '192.168.0.0/24'::cidr AS cidr_column,
              'f'::boolean AS boolean_column,
              'Goodbye, world!'::text AS text_column,
              '2.0, 2.0'::point AS point_column,
              '00000001'::bit(8) AS bit_column,
              '<root><child>more data</child></root>'::xml AS xml_column
          UNION ALL
          SELECT
              3 AS id,
              300 AS amount,
              'processed' AS status,
              '{"key": "value3", "number": 168}'::json AS json_column,
              '{"key": "value3", "number": 168}'::jsonb AS jsonb_column,
              ARRAY[7, 8, 9] AS array_column,
              '22:33:44:55:66:77'::macaddr AS mac_address,
              '192.168.0.3'::inet AS inet_column,
              '192.168.0.0/24'::cidr AS cidr_column,
              't'::boolean AS boolean_column,
              'PostgreSQL is awesome!'::text AS text_column,
              '3.0, 3.0'::point AS point_column,
              '11110000'::bit(8) AS bit_column,
              '<root><child>even more data</child></root>'::xml AS xml_column`,
    measures: {
        count: { type: 'count' },
        total_amount: { type: 'sum', sql: 'amount' }
    },
    dimensions: {
        id: { type: 'number', sql: 'id', primaryKey: true },
        status: { type: 'string', sql: 'status' },
        json: { type: 'string', sql: 'json_column' },
        jsonb: { type: 'string', sql: 'jsonb_column' },
        array: { type: 'string', sql: 'array_column' },
        mac_address: { type: 'string', sql: 'mac_address' },
        inet_column: { type: 'string', sql: 'inet_column' },
        cidr_column: { type: 'string', sql: 'cidr_column' },
        boolean_column: { type: 'string', sql: 'boolean_column' },
        text_column: { type: 'string', sql: 'text_column' },
        point_column: { type: 'string', sql: 'point_column' },
        bit_column: { type: 'string', sql: 'bit_column' },
        xml_column: { type: 'string', sql: 'xml_column' },
    }
});
