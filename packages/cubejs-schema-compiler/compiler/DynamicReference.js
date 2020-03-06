class DynamicReference {
  constructor(memberNames, fn) {
    this.memberNames = memberNames;
    this.fn = fn;
  }
}

module.exports = DynamicReference;
