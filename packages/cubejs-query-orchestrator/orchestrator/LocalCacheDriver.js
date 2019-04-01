class LocalCacheDriver {
  constructor() {
    this.store = {};
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
}

module.exports = LocalCacheDriver;
