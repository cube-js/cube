const CompileError = require('./CompileError');
const UserError = require('./UserError');

exports.handleError = (e) => {
  if (e.status && e.message) {
    return e;
  } else if (e instanceof CompileError) {
    return { status: 400, message: { error: e.messages } };
  } else if (e instanceof UserError) {
    return { status: 400, message: { error: e.message } };
  } else if (e && e.toString().indexOf('ReferenceError') !== -1) { // TODO do all reference check at compile time
    return { status: 400, message: { error: e.message } };
  } else {
    console.error(e.stack || e);
    return { status: 500, message: { error: e.toString() } };
  }
};