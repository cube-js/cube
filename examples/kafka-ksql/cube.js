const { MongoClient } = require('mongodb');

module.exports = {
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 1,
    }
  },
  processSubscriptionsInterval: 1,
  initApp: (app) => {
    app.post('/collect', (req, res) => {
      console.log(req.body);
      const client = new MongoClient(process.env.MONGO_URL);

      client.connect((err) => {
        const db = client.db();
        const collection = db.collection('events');

        collection.insertOne({ timestamp: new Date(), ...req.body }, ((err, result) => {
          client.close();
          res.send('ok');
        }));
      });
    });
  }
};
