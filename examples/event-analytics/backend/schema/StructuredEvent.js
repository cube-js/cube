const escape = (str) => str.replace(/ /g, '%2520')

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

  get categoryEscaped() {
    return escape(this.category);
  }

  get actionEscaped() {
    return escape(this.action);
  }
}

export default StructuredEvent;
