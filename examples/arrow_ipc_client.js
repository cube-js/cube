/**
 * Arrow IPC Client Example for CubeSQL
 *
 * This example demonstrates how to connect to CubeSQL with the Arrow IPC output format
 * and read query results using Apache Arrow's IPC streaming format.
 *
 * Arrow IPC (Inter-Process Communication) is a columnar format that provides:
 * - Zero-copy data transfer
 * - Efficient memory usage for large datasets
 * - Native support in data processing libraries
 *
 * Prerequisites:
 *   npm install pg apache-arrow
 */

const { Client } = require("pg");
const { Table, tableFromJSON } = require("apache-arrow");
const { Readable } = require("stream");

/**
 * CubeSQL Arrow IPC Client
 *
 * Provides methods to connect to CubeSQL and execute queries with Arrow IPC output format.
 */
class CubeSQLArrowIPCClient {
  constructor(config = {}) {
    /**
     * PostgreSQL connection configuration
     * @type {Object}
     */
    this.config = {
      host: config.host || "127.0.0.1",
      port: config.port || 4444,
      user: config.user || "root",
      password: config.password || "",
      database: config.database || "",
    };

    /**
     * Active database connection
     * @type {Client}
     */
    this.client = null;
  }

  /**
   * Connect to CubeSQL server
   * @returns {Promise<void>}
   */
  async connect() {
    this.client = new Client(this.config);

    try {
      await this.client.connect();
      console.log(
        `Connected to CubeSQL at ${this.config.host}:${this.config.port}`
      );
    } catch (error) {
      console.error("Failed to connect to CubeSQL:", error.message);
      throw error;
    }
  }

  /**
   * Enable Arrow IPC output format for this session
   * @returns {Promise<void>}
   */
  async setArrowIPCOutput() {
    try {
      await this.client.query("SET output_format = 'arrow_ipc'");
      console.log("Arrow IPC output format enabled for this session");
    } catch (error) {
      console.error("Failed to set output format:", error.message);
      throw error;
    }
  }

  /**
   * Execute query and return results as array of objects
   * @param {string} query - SQL query to execute
   * @returns {Promise<Array>} Query results as array of objects
   */
  async executeQuery(query) {
    try {
      const result = await this.client.query(query);
      return result.rows;
    } catch (error) {
      console.error("Query execution failed:", error.message);
      throw error;
    }
  }

  /**
   * Execute query with streaming for large result sets
   * @param {string} query - SQL query to execute
   * @param {Function} onRow - Callback function for each row
   * @returns {Promise<number>} Number of rows processed
   */
  async executeQueryStream(query, onRow) {
    return new Promise((resolve, reject) => {
      const cursor = this.client.query(new (require("pg")).Query(query));

      let rowCount = 0;

      cursor.on("row", (row) => {
        onRow(row);
        rowCount++;
      });

      cursor.on("end", () => {
        resolve(rowCount);
      });

      cursor.on("error", reject);
    });
  }

  /**
   * Close connection to CubeSQL
   * @returns {Promise<void>}
   */
  async close() {
    if (this.client) {
      await this.client.end();
      console.log("Disconnected from CubeSQL");
    }
  }
}

/**
 * Example 1: Basic query with Arrow IPC output
 */
async function exampleBasicQuery() {
  console.log("\n=== Example 1: Basic Query with Arrow IPC ===");

  const client = new CubeSQLArrowIPCClient();

  try {
    await client.connect();
    await client.setArrowIPCOutput();

    const query = "SELECT * FROM information_schema.tables LIMIT 10";
    const results = await client.executeQuery(query);

    console.log(`Query: ${query}`);
    console.log(`Rows returned: ${results.length}`);
    console.log("\nFirst few rows:");
    console.log(results.slice(0, 3));
  } finally {
    await client.close();
  }
}

/**
 * Example 2: Stream large result sets
 */
async function exampleStreamResults() {
  console.log("\n=== Example 2: Stream Large Result Sets ===");

  const client = new CubeSQLArrowIPCClient();

  try {
    await client.connect();
    await client.setArrowIPCOutput();

    const query = "SELECT * FROM information_schema.columns LIMIT 1000";
    let rowCount = 0;

    await client.executeQueryStream(query, (row) => {
      rowCount++;
      if (rowCount % 100 === 0) {
        console.log(`Processed ${rowCount} rows...`);
      }
    });

    console.log(`Total rows processed: ${rowCount}`);
  } finally {
    await client.close();
  }
}

/**
 * Example 3: Convert results to JSON and save to file
 */
async function exampleSaveToJSON() {
  console.log("\n=== Example 3: Save Results to JSON ===");

  const client = new CubeSQLArrowIPCClient();
  const fs = require("fs");

  try {
    await client.connect();
    await client.setArrowIPCOutput();

    const query = "SELECT * FROM information_schema.tables LIMIT 50";
    const results = await client.executeQuery(query);

    const outputFile = "/tmp/cubesql_results.json";
    fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));

    console.log(`Query: ${query}`);
    console.log(`Results saved to: ${outputFile}`);
    console.log(`File size: ${fs.statSync(outputFile).size} bytes`);
  } finally {
    await client.close();
  }
}

/**
 * Example 4: Compare performance with and without Arrow IPC
 */
async function examplePerformanceComparison() {
  console.log("\n=== Example 4: Performance Comparison ===");

  const client = new CubeSQLArrowIPCClient();

  try {
    await client.connect();

    const testQuery = "SELECT * FROM information_schema.columns LIMIT 1000";

    // Test with PostgreSQL format (default)
    console.log("\nTesting with PostgreSQL wire format (default):");
    let start = Date.now();
    const resultsPG = await client.executeQuery(testQuery);
    const pgTime = (Date.now() - start) / 1000;
    console.log(`  Rows: ${resultsPG.length}, Time: ${pgTime.toFixed(4)}s`);

    // Test with Arrow IPC
    console.log("\nTesting with Arrow IPC output format:");
    await client.setArrowIPCOutput();
    start = Date.now();
    const resultsArrow = await client.executeQuery(testQuery);
    const arrowTime = (Date.now() - start) / 1000;
    console.log(`  Rows: ${resultsArrow.length}, Time: ${arrowTime.toFixed(4)}s`);

    // Compare
    if (arrowTime > 0) {
      const speedup = pgTime / arrowTime;
      console.log(
        `\nArrow IPC speedup: ${speedup.toFixed(2)}x faster` +
          (speedup > 1
            ? " (Arrow IPC performs better)"
            : " (PostgreSQL format performs better)")
      );
    }
  } finally {
    await client.close();
  }
}

/**
 * Example 5: Process results with native Arrow format
 */
async function exampleArrowNativeProcessing() {
  console.log("\n=== Example 5: Arrow Native Processing ===");

  const client = new CubeSQLArrowIPCClient();

  try {
    await client.connect();
    await client.setArrowIPCOutput();

    const query = "SELECT * FROM information_schema.tables LIMIT 100";
    const results = await client.executeQuery(query);

    // Convert to Arrow Table for columnar processing
    const table = tableFromJSON(results);

    console.log(`Query: ${query}`);
    console.log(`Result: Arrow Table with ${table.numRows} rows and ${table.numCols} columns`);
    console.log("\nColumn names and types:");

    for (let i = 0; i < table.numCols; i++) {
      const field = table.schema.fields[i];
      console.log(`  ${field.name}: ${field.type}`);
    }

    // Example: Get statistics
    console.log("\nExample statistics (if numeric columns exist):");
    for (let i = 0; i < table.numCols; i++) {
      const column = table.getChild(i);
      if (column && column.type.toString() === "Int32") {
        const values = column.toArray();
        const nonNull = values.filter((v) => v !== null);
        if (nonNull.length > 0) {
          const sum = nonNull.reduce((a, b) => a + b, 0);
          const avg = sum / nonNull.length;
          console.log(`  ${table.schema.fields[i].name}: avg=${avg.toFixed(2)}`);
        }
      }
    }
  } finally {
    await client.close();
  }
}

/**
 * Main entry point
 */
async function main() {
  console.log("CubeSQL Arrow IPC Client Examples");
  console.log("=".repeat(50));

  // Check if required packages are installed
  try {
    require("pg");
    require("apache-arrow");
  } catch (error) {
    console.error("Missing required package:", error.message);
    console.log("Install with: npm install pg apache-arrow");
    process.exit(1);
  }

  // Check if CubeSQL is running
  try {
    const testClient = new CubeSQLArrowIPCClient();
    await testClient.connect();
    await testClient.close();
  } catch (error) {
    console.warn("Warning: Could not connect to CubeSQL at 127.0.0.1:4444");
    console.warn(`Error: ${error.message}\n`);
    console.log("To run the examples, start CubeSQL with:");
    console.log(
      "  CUBESQL_CUBE_URL=... CUBESQL_CUBE_TOKEN=... cargo run --bin cubesqld"
    );
    console.log("\nOr run individual examples manually after starting CubeSQL.");
    return;
  }

  // Run examples
  try {
    await exampleBasicQuery();
    await exampleStreamResults();
    await exampleSaveToJSON();
    await examplePerformanceComparison();
    await exampleArrowNativeProcessing();
  } catch (error) {
    console.error("Example execution error:", error);
  }
}

// Run if this is the main module
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  CubeSQLArrowIPCClient,
  exampleBasicQuery,
  exampleStreamResults,
  exampleSaveToJSON,
  examplePerformanceComparison,
  exampleArrowNativeProcessing,
};
