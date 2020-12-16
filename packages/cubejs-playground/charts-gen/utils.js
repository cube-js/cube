const fs = require('fs-extra');
const path = require('path');

function moveRecursively(from, to, includeRegexs = []) {
  fs.readdir(from, (_, files) => {
    files.forEach((name) => {
      const nextFrom = path.join(from, name);
      const nextTo = path.join(to, name);
      if (fs.lstatSync(nextFrom).isDirectory()) {
        moveRecursively(nextFrom, nextTo, includeRegexs);
      } else {
        if (includeRegexs.some((regex) => name.match(regex))) {
          if (!fs.pathExistsSync(path.dirname(nextTo))) {
            fs.mkdirSync(path.dirname(nextTo), { recursive: true });
          }
          fs.copyFile(nextFrom, nextTo);
        }
      }
    });
  });
}

module.exports = {
  moveRecursively
}