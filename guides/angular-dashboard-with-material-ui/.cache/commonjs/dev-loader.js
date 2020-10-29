"use strict";

exports.__esModule = true;
exports.default = void 0;

var _loader = require("./loader");

var _findPath = require("./find-path");

class DevLoader extends _loader.BaseLoader {
  constructor(syncRequires, matchPaths) {
    const loadComponent = chunkName => Promise.resolve(syncRequires.components[chunkName]);

    super(loadComponent, matchPaths);
  }

  loadPage(pagePath) {
    const realPath = (0, _findPath.findPath)(pagePath);
    return super.loadPage(realPath).then(result => require(`./socketIo`).getPageData(realPath).then(() => result));
  }

  loadPageDataJson(rawPath) {
    return super.loadPageDataJson(rawPath).then(data => {
      // when we can't find a proper 404.html we fallback to dev-404-page
      // we need to make sure to mark it as not found.
      if (data.status === _loader.PageResourceStatus.Error && rawPath !== `/dev-404-page/`) {
        console.error(`404 page could not be found. Checkout https://www.gatsbyjs.org/docs/add-404-page/`);
        return this.loadPageDataJson(`/dev-404-page/`).then(result => Object.assign({}, data, result));
      }

      return data;
    });
  }

  doPrefetch(pagePath) {
    return Promise.resolve(require(`./socketIo`).getPageData(pagePath));
  }

}

var _default = DevLoader;
exports.default = _default;