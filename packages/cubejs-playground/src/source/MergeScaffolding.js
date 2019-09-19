import ScaffoldingSources from '../codegen/ScaffoldingSources';
import TargetSource from "./TargetSource";
import SourceSnippet from "./SourceSnippet";

class MergeScaffolding {
  constructor(targetFileName, targetSource, snippet) {
    this.targetFileName = targetFileName;
    this.scaffoldingSourceName = MergeScaffolding.scaffoldingSourceName(targetFileName);
    this.targetSource = targetSource && new TargetSource(targetFileName, targetSource);
    this.snippet = snippet || new SourceSnippet(ScaffoldingSources[this.scaffoldingSourceName]);
  }

  formattedMergeResult() {
    if (!this.targetSource) {
      return ScaffoldingSources[this.scaffoldingSourceName];
    }
    this.snippet.mergeTo(this.targetSource);
    return this.targetSource.formattedCode();
  }

  static scaffoldingSourceName(targetFileName) {
    return targetFileName.replace('/src/', 'react/');
  }

  static targetSourceName(scaffoldingSourceName) {
    return scaffoldingSourceName.replace('react/', '/src/');
  }
}

export default MergeScaffolding;
