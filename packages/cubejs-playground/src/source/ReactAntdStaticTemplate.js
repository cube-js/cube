import TemplatePackage from "./TemplatePackage";
import AppSnippet from "./AppSnippet";
import IndexSnippet from "./IndexSnippet";

class ReactAntdStaticTemplate extends TemplatePackage {
  constructor() {
    super({
      name: 'react-antd-static',
      description: 'React antd static',
      fileToSnippet: {
        '/src/App.js': new AppSnippet(),
        '/src/index.js': new IndexSnippet(),
      }
    });
  }
}

export default ReactAntdStaticTemplate;
