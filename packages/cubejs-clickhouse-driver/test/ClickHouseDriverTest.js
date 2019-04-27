const ClickHouseDriver = require('../driver/ClickHouseDriver');
require('should');
const { GenericContainer } = require("testcontainers");


describe('ClickHouseDriver', () => {

    
    let container, config
    before(async function() {
        this.timeout(20000);

        container = await new GenericContainer("yandex/clickhouse-server")
            .withExposedPorts(8123)
            .start();

        config = {
            host:'localhost',
            port:container.getMappedPort(8123),
        }

    })

    after(async ()=>{
        if (container) {
            await container.stop()
        }
    })

    async function doWithDriver(callback) {
        let driver = new ClickHouseDriver(config)
        try {
            await callback(driver)
        }
        finally {
            await driver.release()
        }
    }

    it('should construct', async () => {
        await doWithDriver(driver=>{})
    })
    it('should test connection', async () => {
        await doWithDriver(async (driver) => {
          await driver.testConnection()
        })
    })
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
    })
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
    })
    it('should get tables', async () => {
        await doWithDriver(async (driver) => {
            let tables = await driver.getTablesQuery("system")
            tables.should.containDeep([
                {table_name:"numbers"},
            ])
        })
    })

    it('should create schema if not exists', async () => {
        await doWithDriver(async (driver) => {
            let name = `temp_${Date.now()}`
            try {
                await driver.createSchemaIfNotExists(name)
            }
            finally {
                await driver.query(`drop database ${name}`)
            }
        })
    })


  })