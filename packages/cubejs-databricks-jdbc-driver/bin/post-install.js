"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require("source-map-support/register");
const shared_1 = require("@cubejs-backend/shared");
const installer_1 = require("./installer");
(async () => {
    try {
        await installer_1.downloadJDBCDriver(true);
    }
    catch (e) {
        await shared_1.displayCLIError(e, 'Cube.js Databricks JDBC Installer');
    }
})();
//# sourceMappingURL=post-install.js.map