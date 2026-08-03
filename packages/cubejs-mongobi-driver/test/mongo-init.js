db = db.getSiblingDB('test');

db.createCollection('mycol');

db.mycol.insertMany([{ number: 1 }]);
