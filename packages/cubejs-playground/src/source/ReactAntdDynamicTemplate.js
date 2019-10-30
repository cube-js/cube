import TemplatePackage from "./TemplatePackage";
import AppSnippet from "./AppSnippet";
import IndexSnippet from "./IndexSnippet";

class ReactAntdDynamicTemplate extends TemplatePackage {
  constructor() {
    super({
      name: 'react-antd-dynamic',
      description: 'React antd dynamic',
      fileToSnippet: {
        '/src/App.js': new AppSnippet(),
        '/src/index.js': new IndexSnippet()
      }
    });
  }
}

export default ReactAntdDynamicTemplate;
