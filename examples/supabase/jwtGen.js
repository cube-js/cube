const jwt = require('jsonwebtoken');
const CUBE_API_SECRET = '<Your-Token>';

const cubejsToken = jwt.sign(
    {}, CUBE_API_SECRET, { expiresIn: '30d' }
);

console.log(cubejsToken);