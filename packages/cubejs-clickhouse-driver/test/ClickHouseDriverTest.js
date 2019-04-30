const ClickHouseDriver = require('../driver/ClickHouseDriver');
require('should');
const { GenericContainer } = require("testcontainers");

describe('ClickHouseDriver', () => {
    let container, config;

    const doWithDriver = async (callback) => {
        let driver = new ClickHouseDriver(config);

        try {
            await callback(driver)
        } finally {
            await driver.release()
        }
    };

    before(async function() {
        this.timeout(20000);

        container = await new GenericContainer("yandex/clickhouse-server")
            .withExposedPorts(8123)
            .start();

        config = {
            host: 'localhost',
            port: container.getMappedPort(8123),
        };
    });

    after(async () => {
        if (container) {
            await container.stop()
        }
    });

    it('should construct', async () => {
        await doWithDriver(driver => {})
    });

    it('should test connection', async () => {
        await doWithDriver(async (driver) => {
          await driver.testConnection()
        })
    });

    it('should select raw sql', async () => {
        await doWithDriver(async (driver) => {
            let numbers = await driver.query("SELECT number FROM system.numbers LIMIT 10")
            numbers.should.be.deepEqual([ 
                { number: '0' },
                { number: '1' },
                { number: '2' },
                { number: '3' },
                { number: '4' },
                { number: '5' },
                { number: '6' },
                { number: '7' },
                { number: '8' },
                { number: '9' },
            ])
        })
    });

    it('should select raw sql multiple times', async () => {
        await doWithDriver(async (driver) => {
            let numbers = await driver.query("SELECT number FROM system.numbers LIMIT 5")
            numbers.should.be.deepEqual([ 
                { number: '0' },
                { number: '1' },
                { number: '2' },
                { number: '3' },
                { number: '4' },
            ])
            numbers = await driver.query("SELECT number FROM system.numbers LIMIT 5")
            numbers.should.be.deepEqual([ 
                { number: '0' },
                { number: '1' },
                { number: '2' },
                { number: '3' },
                { number: '4' },
            ])
        })
    });

    it('should get tables', async () => {
        await doWithDriver(async (driver) => {
            let tables = await driver.getTablesQuery("system")
            tables.should.containDeep([
                {table_name:"numbers"},
            ])
        })
    });

    it('should create schema if not exists', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name)
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });

    // Int8
    // Int16
    // Int32
    // Int64
    // UInt8
    // UInt16
    // UInt32
    // UInt64
    // Float32
    // Float64
    it('should normalise all numbers as strings', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name);
                await driver.query(`CREATE TABLE ${name}.a (int8 Int8, int16 Int16, int32 Int32, int64 Int64, uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, float32 Float32, float64 Float64) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.a VALUES (1,1,1,1,1,1,1,1,1,1)`);

                const values = await driver.query(`SELECT * FROM ${name}.a`);
                values.should.deepEqual([{
                    int8: '1',
                    int16: '1',
                    int32: '1',
                    int64: '1',
                    uint8: '1',
                    uint16: '1',
                    uint32: '1',
                    uint64: '1',
                    float32: '1',
                    float64: '1',                
                }])
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });

    it('should normalise all dates as ISO8601', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name);
                await driver.query(`CREATE TABLE ${name}.a (dateTime DateTime, date Date) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.a VALUES ('2019-04-30 11:55:00', '2019-04-30')`);

                const values = await driver.query(`SELECT * FROM ${name}.a`);
                values.should.deepEqual([{
                    dateTime: '2019-04-30T11:55:00.000Z',
                    date: '2019-04-30T00:00:00.000Z',
                }])
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });

    it('should substitute parameters', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name);
                await driver.query(`CREATE TABLE ${name}.test (x Int32, s String) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.test VALUES (?, ?), (?, ?), (?, ?)`, [1, "str1", 2, "str2", 3, "str3"]);
                const values = await driver.query(`SELECT * FROM ${name}.test WHERE x = ?`, 2);
                values.should.deepEqual([{x: '2', s: "str2"}])
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });

    it('should return null for missing values on left outer join', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name);
                await driver.query(`CREATE TABLE ${name}.a (x Int32, s String) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.a VALUES (?, ?), (?, ?), (?, ?)`, [1, 'str1', 2, 'str2', 3, 'str3']);

                await driver.query(`CREATE TABLE ${name}.b (x Int32, s String) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.b VALUES (?, ?), (?, ?), (?, ?)`, [2, 'str2', 3, 'str3', 4, 'str4']);

                const values = await driver.query(`SELECT * FROM ${name}.a LEFT OUTER JOIN ${name}.b ON a.x = b.x`);
                values.should.deepEqual([
                    { x: '1', s: 'str1', 'b.x': '0', 'b.s': null },
                    { x: '2', s: 'str2', 'b.x': '2', 'b.s': 'str2' },
                    { x: '3', s: 'str3', 'b.x': '3', 'b.s': 'str3' }
                ])
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });


  });
