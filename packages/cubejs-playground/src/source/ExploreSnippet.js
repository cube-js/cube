import traverse from "@babel/traverse";
import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';

class ExploreSnippet extends SourceSnippet {
  constructor() {
    super(ScaffoldingSources['react/ExplorePage.js']);
  }
}

export default ExploreSnippet;
