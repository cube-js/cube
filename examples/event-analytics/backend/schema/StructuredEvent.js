class StructuredEvent {
  constructor(category, action) {
    this.category = category;
    this.action = action;
  }

  get humanName() {
    return `${this.category}: ${this.action}`;
  }

  get systemName() {
    return [this.category, this.action].join("__").replace(/ /g, "_");
  }
}

export default StructuredEvent;
