"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.downloadJDBCDriver = void 0;
const path_1 = __importDefault(require("path"));
const inquirer_1 = __importDefault(require("inquirer"));
const shared_1 = require("@cubejs-backend/shared");
function acceptedByEnv() {
    const acceptStatus = shared_1.getEnv('databrickAcceptPolicy');
    if (acceptStatus) {
        console.log('You accepted Terms & Conditions for JDBC driver from DataBricks by CUBEJS_DB_DATABRICKS_ACCEPT_POLICY');
    }
    if (acceptStatus === false) {
        console.log('You declined Terms & Conditions for JDBC driver from DataBricks by CUBEJS_DB_DATABRICKS_ACCEPT_POLICY');
        console.log('Installation will be skipped');
    }
    return acceptStatus;
}
async function cliAcceptVerify() {
    console.log('Databricks driver is using JDBC driver from Data Bricks');
    console.log('By downloading the driver, you agree to the Terms & Conditions');
    console.log('https://databricks.com/jdbc-odbc-driver-license');
    console.log('More info: https://databricks.com/spark/jdbc-drivers-download');
    if (process.stdout.isTTY) {
        const { licenseAccepted } = await inquirer_1.default.prompt([{
                type: 'confirm',
                name: 'licenseAccepted',
                message: 'You read & agree to the Terms & Conditions',
            }]);
        return licenseAccepted;
    }
    shared_1.displayCLIWarning('Your stdout is not interactive, you can accept it via CUBEJS_DB_DATABRICKS_ACCEPT_POLICY=true');
    return false;
}
async function downloadJDBCDriver(isCli = false) {
    let driverAccepted = acceptedByEnv();
    if (driverAccepted === undefined && isCli) {
        driverAccepted = await cliAcceptVerify();
    }
    if (driverAccepted) {
        console.log('Downloading SimbaSparkJDBC42-2.6.17.1021');
        await shared_1.downloadAndExtractFile('https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/SimbaSparkJDBC42-2.6.17.1021.zip', {
            showProgress: true,
            cwd: path_1.default.resolve(path_1.default.join(__dirname, '..', '..', 'download')),
        });
        console.log('Release notes: https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/jdbc/2.6.17/docs/release-notes.txt');
        return path_1.default.resolve(path_1.default.join(__dirname, '..', '..', 'download', 'SparkJDBC42.jar'));
    }
    return null;
}
exports.downloadJDBCDriver = downloadJDBCDriver;
//# sourceMappingURL=installer.js.map