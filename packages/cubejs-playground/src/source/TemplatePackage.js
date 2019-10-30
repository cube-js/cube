class TemplatePackage {
  constructor({ name, description, fileToSnippet }) {
    this.name = name;
    this.description = description;
    this.fileToSnippet = fileToSnippet;
  }

  initSources(scaffoldingSources) {
    this.templateSources = Object.keys(scaffoldingSources).filter(f => f.indexOf(`${this.name}/`) === 0).map(f => ({
      [f.substring(this.name.length)]: scaffoldingSources[f]
    })).reduce((a, b) => ({ ...a, ...b }), {});
    Object.keys(this.fileToSnippet).forEach(file => {
      if (this.templateSources[file]) {
        this.fileToSnippet[file].source = this.templateSources[file];
      }
    });
  }
}

export default TemplatePackage;
