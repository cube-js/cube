import { JoinGraph } from '../../src/compiler/JoinGraph';

class TestJoinGraph extends JoinGraph {
  public static addNodeEdge(nodes: Record<string, Record<string, 1>>, from: string, to: string): void {
    super.addNodeEdge(nodes, from, to);
  }
}

describe('JoinGraph node names inherited from Object.prototype', () => {
  it.each(['constructor', 'toString', 'hasOwnProperty'])('gives %s its own node', (name) => {
    const nodes: Record<string, Record<string, 1>> = {};

    TestJoinGraph.addNodeEdge(nodes, name, 'orders');
    TestJoinGraph.addNodeEdge(nodes, name, 'users');

    expect(nodes).toEqual({ [name]: { orders: 1, users: 1 } });
    expect(Object.hasOwn(Object, 'orders')).toBe(false);
    expect(Object.hasOwn(Function.prototype, 'orders')).toBe(false);
  });
});
