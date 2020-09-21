const varsJSON = require('./raw-variables');

function toCamelCase(str) {
  return str.replace(/-[a-z0-9]/g, s => s.replace('-', '').toUpperCase());
}

const vars = {};

// Resolve variable links and create LESS variable map.
Object.keys(varsJSON)
  .forEach((key) => {
    let value = varsJSON[key];

    while (value.includes('@')) {
      const newValue = value.replace(/@([a-z0-9-]+)/gi, (s, s1) => {
        return varsJSON[s1] || s;
      });

      if (newValue !== value) {
        value = newValue;
      } else {
        break;
      }
    }

    vars[toCamelCase(key)] = value;
  });

module.exports = vars;
