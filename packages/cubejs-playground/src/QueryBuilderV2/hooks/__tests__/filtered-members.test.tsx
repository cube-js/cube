import { createElement } from 'react';
import { renderToString } from 'react-dom/server';

import { TCubeFolder } from '../../types';
import { useFilteredMembers } from '../filtered-members';

function getFolderOrder(folders: TCubeFolder[], filterString = '') {
  let result: string[] = [];

  function Probe() {
    result = useFilteredMembers(filterString, {
      dimensions: [],
      measures: [],
      segments: [],
      folders,
      hierarchies: [],
    }).folders.map((folder) => folder.name);

    return null;
  }

  renderToString(createElement(Probe));

  return result;
}

describe('useFilteredMembers', () => {
  // https://github.com/cube-js/cube/issues/12872
  it('keeps numerically prefixed folder names in order', () => {
    const folders = [
      { name: '1. B', members: [] },
      { name: '2. A', members: [] },
    ] as unknown as TCubeFolder[];

    expect(getFolderOrder(folders)).toEqual(['1. B', '2. A']);
  });

  it('keeps folders with three numeric prefixes in order', () => {
    const folders = [
      { name: '1. Metrics', members: [] },
      { name: '2. Events', members: [] },
      { name: '3. Pages', members: [] },
    ] as unknown as TCubeFolder[];

    expect(getFolderOrder(folders)).toEqual(['1. Metrics', '2. Events', '3. Pages']);
  });
});
