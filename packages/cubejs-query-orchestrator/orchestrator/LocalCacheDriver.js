const store = {};

class LocalCacheDriver {
  constructor() {
    this.store = store;
  }

  async get(key) {
    if (this.store[key] && this.store[key].exp < new Date().getTime()) {
      delete this.store[key];
    }
    return this.store[key] && this.store[key].value;
  }

  async set(key, value, expiration) {
    this.store[key] = {
      value,
      exp: new Date().getTime() + expiration * 1000
    };
  }

  async remove(key) {
    delete this.store[key];
  }

  async keysStartingWith(prefix) {
    return Object.keys(this.store)
      .filter(k => k.indexOf(prefix) === 0 && this.store[k].exp > new Date().getTime());
  }
}

module.exports = LocalCacheDriver;
