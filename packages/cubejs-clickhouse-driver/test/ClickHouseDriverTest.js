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

    it('should substitute parameters', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name);
                await driver.query(`CREATE TABLE ${name}.test (x Int32, s String) ENGINE Log`);
                await driver.query(`INSERT INTO ${name}.test VALUES (?, ?), (?, ?), (?, ?)`, [1, "str1", 2, "str2", 3, "str3"]);
                const values = await driver.query(`SELECT * FROM ${name}.test WHERE x = ?`, 2);
                values.should.deepEqual([{x: 2, s: "str2"}])
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
                    { x: 1, s: 'str1', 'b.x': 0, 'b.s': null },
                    { x: 2, s: 'str2', 'b.x': 2, 'b.s': 'str2' },
                    { x: 3, s: 'str3', 'b.x': 3, 'b.s': 'str3' }
                ])
            }
            finally {
                await driver.query(`DROP DATABASE ${name}`)
            }
        })
    });
  });
