const { MongoClient } = require('mongodb');

module.exports = {
  processSubscriptionsInterval: 1000,
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 1,
    }
  },
  initApp: (app) => {
    app.post('/collect', async (req, res, next) => {
      const client = new MongoClient(process.env.MONGO_URL);

      try {
        await client.connect();
        const database = client.db();
        const collection = database.collection('events');
        await collection.insertOne({ timestamp: new Date(), ...req.body });
        await client.close();
        res.send('ok');
      } catch (err) {
        await client.close();
        next(err);
      }

    });
  }
};
