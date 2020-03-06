const TemplatePackage = require("../../TemplatePackage");

class WebSocketTransportTemplate extends TemplatePackage {
  constructor() {
    super({
      name: 'web-socket-transport',
      version: '0.0.1',
      type: 'transport'
    });
  }
}

module.exports = WebSocketTransportTemplate;
