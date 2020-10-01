"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// eslint-disable-next-line no-undef
exports.default = (async () => {
    const apiSecret = await (async () => 'secret')();
    const configuration = {
        dbType: 'postgres',
        apiSecret,
    };
    return configuration;
})();
