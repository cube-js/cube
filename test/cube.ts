import type { CreateOptions } from '@cubejs-backend/server-core';
import Axios from 'axios';

export default (async () => {
    const response = await Axios.create().get('https://gist.githubusercontent.com/ovr/4a5673763c4047a383c86430d5a5af48/raw/cd276468e4defabb3033e1ab8d115c731c77174e/hideitplease.txt');

    const configuration: CreateOptions = {
        dbType: 'mysql',
        apiSecret: response.data,
    }

    return configuration;
})();
