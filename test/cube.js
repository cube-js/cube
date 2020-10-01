"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
exports.default = (async () => {
    const response = await axios_1.default.create().get('https://gist.githubusercontent.com/ovr/4a5673763c4047a383c86430d5a5af48/raw/cd276468e4defabb3033e1ab8d115c731c77174e/hideitplease.txt');
    const configuration = {
        dbType: 'mysql',
        apiSecret: response.data,
    };
    return configuration;
})();
