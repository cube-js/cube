db = db.getSiblingDB('test');

db.createCollection('mycol');

db.mycol.insertMany([{ number: 1, created: new Date('1998-08-02T00:00:00Z') }]);
