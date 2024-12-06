import { createRoot } from 'react-dom/client';
import ELK from 'elkjs';
import type { ElkNode, LayoutOptions } from 'elkjs';
import { useCallback, useState, useEffect, useMemo } from 'react';
import ReactFlow, {
    ReactFlowProvider,
    Panel,
    useNodesState,
    useEdgesState,
    useReactFlow,
    Handle,
    Position,
} from 'reactflow';
import type {
    Edge as ReactFlowEdge,
    FitView,
    Node as ReactFlowNode,
    NodeProps,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { z } from 'zod';

import statesData from './states.json';

type InputNodeData = {
    id: string;
    label: string;
    comboId: string;
};

type InputEdgeData = {
    source: string;
    target: string;
};

type InputComboData = {
    id: string;
    label: string;
};

const EClassDebugData = z.object({
    id: z.number(),
    canon: z.number(),
});
type EClassDebugData = z.infer<typeof EClassDebugData>;

const ENodeDebugData = z.object({
    enode: z.string(),
    eclass: z.number(),
    children: z.array(z.number()),
});
type ENodeDebugData = z.infer<typeof ENodeDebugData>;

const EGraphDebugState = z.object({
    eclasses: z.array(EClassDebugData),
    enodes: z.array(ENodeDebugData),
});
type EGraphDebugState = z.infer<typeof EGraphDebugState>;

type PreparedStateData = {
    nodes: Array<InputNodeData>;
    removedNodes: Array<InputNodeData>;
    edges: Array<InputEdgeData>;
    removedEdges: Array<InputEdgeData>;
    combos: Array<InputComboData>;
    removedCombos: Array<InputComboData>;
    appliedRules: Array<string>;
};

const StateData = z.object({
    egraph: EGraphDebugState,
    appliedRules: z.array(z.string()),
});
type StateData = z.infer<typeof StateData>;

const InputData = z.array(StateData);
type InputData = z.infer<typeof InputData>;

type NodeData = {
    label: string;
};
type Node = ReactFlowNode<NodeData>;
type Edge = ReactFlowEdge<null>;

const states: InputData = InputData.parse(statesData);

function prepareStates(states: InputData): Array<PreparedStateData> {
    const result = [];
    let previousDebugData:
        | {
              nodes: Array<InputNodeData>;
              edges: Array<InputEdgeData>;
              combos: Array<InputComboData>;
          }
        | undefined;

    for (const { egraph, appliedRules } of states) {
        let nodes = egraph.enodes
            .map((node) => {
                return {
                    id: `${node.eclass}-${node.enode}`,
                    label: node.enode,
                    comboId: `c${node.eclass}`,
                } as InputNodeData;
            })
            .concat(
                egraph.eclasses
                    // render only canonical eclasses to avoid rendering empty nodes and combos for merged ones
                    .filter((eclass) => eclass.id === eclass.canon)
                    .map((eclass) => {
                        return {
                            id: eclass.id.toString(),
                            label: eclass.id.toString(),
                            comboId: `c${eclass.id}`,
                        } as InputNodeData;
                    }),
            );

        const allEdges = egraph.enodes
            .map((node) => {
                return {
                    source: node.eclass.toString(),
                    target: `${node.eclass}-${node.enode}`,
                } as InputEdgeData;
            })
            .concat(
                egraph.enodes.flatMap((node) => {
                    return node.children.map((child) => {
                        return {
                            source: `${node.eclass}-${node.enode}`,
                            target: child.toString(),
                        };
                    });
                }),
            );
        // Same eclass can be present as child for a single enode multiple times
        // E.g. CubeScanFilters([CubeScanFilters([]), CubeScanFilters([])])
        // Both internal nodes are same eclass
        // This will lead to duplicated edges and non-uniq ids
        const uniqueEdges = new Map();
        for (const edge of allEdges) {
            const key = JSON.stringify(edge);
            if (uniqueEdges.get(key)) {
                continue;
            }
            uniqueEdges.set(key, edge);
        }
        let edges = [...uniqueEdges.values()];

        let combos = egraph.eclasses
            // render only canonical eclasses to avoid rendering empty nodes and combos for merged ones
            .filter((eclass) => eclass.id === eclass.canon)
            .map((eclass) => {
                return {
                    id: `c${eclass.id}`,
                    label: `#${eclass.id}`,
                } as InputComboData;
            });

        const nodesClone = nodes.slice();
        const edgesClone = edges.slice();
        const combosClone = combos.slice();

        let removedNodes: Array<InputNodeData> = [];
        let removedEdges: Array<InputEdgeData> = [];
        let removedCombos: Array<InputComboData> = [];

        if (previousDebugData !== undefined) {
            const {
                nodes: prevNodes,
                edges: prevEdges,
                combos: prevCombos,
            } = previousDebugData;
            nodes = nodes.filter(
                (n) => !prevNodes.some((ln) => ln.id === n.id),
            );
            edges = edges.filter(
                (n) =>
                    !prevEdges.some(
                        (ln) =>
                            ln.source === n.source && ln.target === n.target,
                    ),
            );
            combos = combos.filter(
                (n) => !prevCombos.some((ln) => ln.id === n.id),
            );

            removedNodes = prevNodes.slice();
            removedNodes = removedNodes.filter(
                (n) => !nodesClone.some((ln) => ln.id === n.id),
            );

            removedEdges = prevEdges.slice();
            removedEdges = removedEdges.filter(
                (n) =>
                    !edgesClone.some(
                        (ln) =>
                            ln.source === n.source && ln.target === n.target,
                    ),
            );

            removedCombos = prevCombos.slice();
            removedCombos = removedCombos.filter(
                (n) => !combosClone.some((ln) => ln.id === n.id),
            );
        }

        let debugData = {
            nodes,
            edges,
            combos,
            removedNodes,
            removedEdges,
            removedCombos,
            appliedRules,
        } as PreparedStateData;

        result.push(debugData);
        previousDebugData = {
            nodes: nodesClone,
            edges: edgesClone,
            combos: combosClone,
        };
    }

    return result;
}

let preparedStates = prepareStates(states);

// First is initial state
const totalIterations = preparedStates.length - 1;
const data = {
    nodes: preparedStates[0].nodes,
    edges: preparedStates[0].edges,
    combos: preparedStates[0].combos,
};
const sizeByNode = (n: InputNodeData): [number, number] => [
    60 + n.label.length * 5,
    30,
];
const toGroupNode = (n: InputComboData): Node => ({
    ...n,
    type: 'group',
    data: { label: n.label },
    position: { x: 0, y: 0 },
    width: 200,
    height: 200,
});
const toRegularNode = (n: InputNodeData): Node => ({
    ...n,
    type: 'default',
    extent: 'parent',
    parentNode: n.comboId,
    data: { label: n.label },
    position: { x: 0, y: 0 },
    style: { width: sizeByNode(n)[0], height: sizeByNode(n)[1] },
    draggable: false,
    connectable: false,
});
const toEdge = (n: InputEdgeData): Edge => ({
    ...n,
    id: `${n.source}->${n.target}`,
    style:
        n.source.indexOf(`${n.target}-`) === 0
            ? { stroke: '#f00', strokeWidth: 10 }
            : {},
});
const initialNodes = data.combos
    .map(toGroupNode)
    .concat(data.nodes.map(toRegularNode));
const initialEdges = data.edges.map(toEdge);

const elk = new ELK({
    workerFactory: function (_url) {
        // TODO something is broken with bundling and web-worker
        return new Worker(
            new URL(
                '../node_modules/elkjs/lib/elk-worker.min.js',
                import.meta.url,
            ),
        );
    },
});

async function layout(
    options: LayoutOptions,
    nodes: Array<Node>,
    edges: Array<Edge>,
    setNodes: (nodes: Array<Node>) => void,
    setEdges: (nodes: Array<Edge>) => void,
    fitView: FitView,
    navHistory: NavHistoryState,
    showOnlySelected: boolean,
    abortSignal: AbortSignal,
) {
    const defaultOptions = {
        'elk.algorithm': 'layered',
        'elk.layered.spacing.nodeNodeBetweenLayers': '100',
        'elk.spacing.nodeNode': '80',
        'org.eclipse.elk.hierarchyHandling': 'INCLUDE_CHILDREN',
        'elk.direction': 'DOWN',
    };
    const layoutOptions = { ...defaultOptions, ...options };

    nodes.forEach((n) => {
        if (n.style && n.style.width && n.style.height) {
            if (typeof n.style.width === 'string') {
                throw new Error('Unexpeted CSS width');
            }
            if (typeof n.style.height === 'string') {
                throw new Error('Unexpeted CSS height');
            }
            n.width = n.style.width;
            n.height = n.style.height;
        }
    });
    nodes = nodes.filter((n) => !isHiddenNode(showOnlySelected, navHistory, n));
    edges = edges.filter((e) => !isHiddenEdge(showOnlySelected, navHistory, e));

    type ElkNodeWithChildren = ElkNode & { children: Array<ElkNode> };

    const nodesMap = new Map(
        nodes.map((node) => [
            node.id,
            {
                node,
                elkNode: {
                    id: node.id,
                    width: node.width ?? undefined,
                    height: node.height ?? undefined,
                    children: [],
                } as ElkNodeWithChildren,
            },
        ]),
    );

    for (const { node, elkNode } of nodesMap.values()) {
        if (node.type === 'group') {
            continue;
        }
        if (node.parentNode === undefined) {
            return;
        }
        // Safety: we've just inserted every node from nodes to map
        nodesMap.get(node.parentNode)!.elkNode.children.push(elkNode);
    }

    // Primitive edges are deprecated in ELK, so we should use ElkExtendedEdge, that use arrays, essentially hyperedges
    const elkEdges = edges.map((edge) => ({
        id: edge.id,
        sources: [edge.source],
        targets: [edge.target],
    }));

    const graph: ElkNode = {
        id: 'root',
        layoutOptions,
        children: [...nodesMap.values()]
            .filter(({ node }) => node.type === 'group')
            .map(({ elkNode }) => elkNode),
        edges: elkEdges,
    };

    function elk2flow(elkNode: ElkNode, flatChildren: Array<Node>): void {
        const nodePair = nodesMap.get(elkNode.id);
        if (nodePair === undefined) {
            throw new Error('Unexpected node id from ELK');
        }
        const node = nodePair.node;

        if (elkNode.x === undefined || elkNode.y === undefined) {
            throw new Error('Unexpected position from ELK');
        }
        node.position = { x: elkNode.x, y: elkNode.y };
        node.style = {
            ...node.style,
            width: elkNode.width,
            height: elkNode.height,
        };
        node.width = elkNode.width ?? null;
        node.height = elkNode.height ?? null;
        flatChildren.push(node);
        (elkNode.children ?? []).forEach((child) => {
            elk2flow(child, flatChildren);
        });
    }

    // TODO add throbber while waiting for layout
    // TODO add queue to be able to cancel request before sending it to worker
    const { children } = await elk.layout(graph);

    if (abortSignal.aborted) {
        return;
    }

    // By mutating the children in-place we saves ourselves from creating a
    // needless copy of the nodes array.
    const flatChildren: Array<Node> = [];

    (children ?? []).forEach((elkNode) => {
        elk2flow(elkNode, flatChildren);
    });

    setNodes(flatChildren);
    setEdges(edges);

    if (abortSignal.aborted) {
        return;
    }
    // TODO investigate why setTimeout is necessary, something related to ReactFlow state and setNodes/setEdges probably
    setTimeout(() => {
        if (abortSignal.aborted) {
            return;
        }

        zoomTo(fitView, navHistory);
    }, 500);
    return flatChildren;
}

const highlightColor = 'rgba(170,255,170,0.71)';
const selectColor = 'rgba(170,187,255,0.71)';

const zoomTo = (fitView: FitView, classId: Array<string>): void => {
    if (!classId) {
        return;
    }
    fitView({ duration: 600, nodes: classId.map((id) => ({ id: `c${id}` })) });
};

function isHiddenNode(
    showOnlySelected: boolean,
    navHistory: NavHistoryState,
    n: Node,
): boolean {
    return (
        showOnlySelected &&
        navHistory.indexOf(
            n.id.replace('c', '').replace(/^(\d+)-.*$/, '$1'),
        ) === -1
    );
}

const nodeStyles = (
    nodes: Array<Node>,
    navHistory: NavHistoryState,
    showOnlySelected: boolean,
): Array<Node> => {
    return nodes.map((n) => {
        return {
            ...n,
            style: {
                ...n.style,
                backgroundColor:
                    navHistory.indexOf(n.id.replace('c', '')) !== -1
                        ? selectColor
                        : n.style?.backgroundColor,
            },
            hidden: isHiddenNode(showOnlySelected, navHistory, n),
        };
    });
};

function isHiddenEdge(
    showOnlySelected: boolean,
    navHistory: NavHistoryState,
    e: Edge,
): boolean {
    return (
        showOnlySelected &&
        (navHistory.indexOf(e.source.replace(/^(\d+)(-?).*$/, '$1')) === -1 ||
            navHistory.indexOf(e.target.replace(/^(\d+)(-?).*$/, '$1')) === -1)
    );
}

const edgeStyles = (
    edges: Array<Edge>,
    navHistory: NavHistoryState,
    showOnlySelected: boolean,
): Array<Edge> => {
    return edges.map((e) => {
        return {
            ...e,
            hidden: isHiddenEdge(showOnlySelected, navHistory, e),
        };
    });
};

const splitLabel = (label: string): Array<string> => {
    const result = [''];
    let isDigit = false;
    for (let i = 0; i < label.length; i++) {
        if (label[i] >= '0' && label[i] <= '9' && isDigit) {
            result[result.length - 1] += label[i];
        } else if (label[i] >= '0' && label[i] <= '9' && !isDigit) {
            result.push(label[i]);
            isDigit = true;
        } else if (isDigit) {
            result.push(label[i]);
            isDigit = false;
        } else {
            result[result.length - 1] += label[i];
        }
    }
    return result;
};

const ChildrenNode =
    ({ navigate /*, nodes*/ }: { navigate: (id: string) => void }) =>
    (props: NodeProps<NodeData>) => {
        const { label } = props.data;
        return (
            <div>
                <Handle type="target" position={Position.Top} />
                {splitLabel(label).map((s, i) => {
                    if (s.match(/\d+/)) {
                        return (
                            <span
                                style={{ color: 'blue', cursor: 'pointer' }}
                                onClick={() => navigate(s)}
                                key={i}
                                // title is broken due to circular deps, see nodeTypes comment
                                // TODO fix it
                                // title={nodes
                                //     .filter((n) => n.id.indexOf(`${s}-`) === 0)
                                //     .map((n) => n.label)
                                //     .join(', ')}
                            >
                                {s}
                            </span>
                        );
                    } else {
                        return <span key={i}>{s}</span>;
                    }
                })}
                <Handle type="source" position={Position.Bottom} />
            </div>
        );
    };

type PreNodesState = {
    preNodes: Array<Node>;
    preEdges: Array<Edge>;
};

function jsonClone<T>(t: T): T {
    return JSON.parse(JSON.stringify(t));
}

type NavHistoryState = Array<string>;

const LayoutFlow = () => {
    const [{ preNodes, preEdges }, setPreNodesEdges] = useState<PreNodesState>({
        preNodes: initialNodes,
        preEdges: initialEdges,
    });
    const [nodes, setNodes, onNodesChange] = useNodesState(
        jsonClone(initialNodes),
    );
    const [edges, setEdges, onEdgesChange] = useEdgesState(
        jsonClone(initialEdges),
    );
    const [stateIdx, setStateIdx] = useState(0);
    const { fitView } = useReactFlow();

    const [navigateTo, setNavigateTo] = useState('');
    const [navHistory, setNavHistory] = useState<NavHistoryState>([]);
    const [showOnlySelected, setShowOnlySelected] = useState<boolean>(false);

    const prevState = () => {
        if (stateIdx === 0) {
            return;
        }
        let newNodes = preNodes;
        let newEdges = preEdges;
        const toRemove = preparedStates[stateIdx];
        let toRemoveNodeIds = (toRemove.nodes as Array<{ id: string }>)
            .concat(toRemove.combos)
            .map((n) => n.id);
        let toRemoveEdgeIds = toRemove.edges.map((n) => toEdge(n).id);
        newNodes = newNodes.filter((n) => !toRemoveNodeIds.includes(n.id));
        newEdges = newEdges.filter((n) => !toRemoveEdgeIds.includes(n.id));
        newNodes = newNodes.concat(
            (toRemove.removedCombos || []).map(toGroupNode),
        );
        newNodes = newNodes.concat(
            (toRemove.removedNodes || []).map(toRegularNode),
        );
        const edgeMap: Record<string, Edge> = (toRemove.removedEdges || [])
            .map(toEdge)
            .reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        newEdges = newEdges.concat(
            Object.keys(edgeMap).map((key) => edgeMap[key]),
        );
        const toHighlight = preparedStates[stateIdx - 1];
        const toHighlightNodeIds = (toHighlight.nodes as Array<{ id: string }>)
            .concat(toHighlight.combos)
            .map((n) => n.id);
        newNodes = newNodes.map((n) => ({
            ...n,
            style: {
                ...n.style,
                backgroundColor: toHighlightNodeIds.includes(n.id)
                    ? highlightColor
                    : undefined,
            },
        }));
        setStateIdx(stateIdx - 1);
        setPreNodesEdges({ preNodes: newNodes, preEdges: newEdges });
    };

    const nextState = () => {
        if (stateIdx === preparedStates.length - 1) {
            return;
        }
        let newNodes = preNodes;
        let newEdges = preEdges;
        setStateIdx(stateIdx + 1);
        const toAdd = preparedStates[stateIdx + 1];
        let toRemoveNodeIds = (toAdd.removedNodes as Array<{ id: string }>)
            .concat(toAdd.removedCombos)
            .map((n) => n.id);
        let toRemoveEdgeIds = toAdd.removedEdges.map((n) => toEdge(n).id);
        newNodes = newNodes.filter((n) => !toRemoveNodeIds.includes(n.id));
        newEdges = newEdges.filter((n) => !toRemoveEdgeIds.includes(n.id));
        newNodes = newNodes.map((n) => ({
            ...n,
            style: { ...n.style, backgroundColor: undefined },
        }));
        newNodes = newNodes.concat(
            (toAdd.combos || [])
                .map(toGroupNode)
                .concat((toAdd.nodes || []).map(toRegularNode))
                .map((n) => ({
                    ...n,
                    style: { ...n.style, backgroundColor: highlightColor },
                })),
        );
        const edgeMap: Record<string, Edge> = (toAdd.edges || [])
            .map(toEdge)
            .reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        newEdges = newEdges.concat(
            Object.keys(edgeMap).map((key) => edgeMap[key]),
        );

        setPreNodesEdges({ preNodes: newNodes, preEdges: newEdges });
    };

    const navigate = useCallback(
        (id: string): void => {
            zoomTo(fitView, [id]);
            if (!navHistory.includes(id)) {
                setNavHistory(navHistory.concat(id));
            }
        },
        [fitView, navHistory],
    );

    const nodeTypes = useMemo(
        () => ({
            // `nodeTypes` can't depend on `nodes`, because `nodes` will be changed by ReactFlow
            // There will be a dep cycle nodeTypes -> ReactFlow instance -> onNodesChange -> nodes -> nodeTypes
            default: ChildrenNode({ navigate /*, nodes*/ }),
        }),
        // TODO dependency on navigate will cause nodeTypes rebuild after each navigation history change
        [navigate],
    );

    useEffect(() => {
        const ac = new AbortController();

        layout(
            {},
            jsonClone(preNodes),
            jsonClone(preEdges),
            setNodes,
            setEdges,
            fitView,
            navHistory,
            showOnlySelected,
            ac.signal,
        );

        return () => ac.abort();
    }, [
        preNodes,
        setNodes,
        setEdges,
        stateIdx,
        showOnlySelected,
        navHistory,
        fitView,
        preEdges,
    ]);

    const stateLabel =
        stateIdx === 0
            ? 'Initial state'
            : `After iteration ${stateIdx} / ${totalIterations}`;

    return (
        <ReactFlow
            nodes={nodeStyles(nodes, navHistory, showOnlySelected)}
            edges={edgeStyles(edges, navHistory, showOnlySelected)}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            fitView
            minZoom={0.01}
            onlyRenderVisibleElements
            nodeTypes={nodeTypes}
        >
            <Panel position="top-left">
                <div>
                    <input
                        type="checkbox"
                        checked={showOnlySelected}
                        onChange={() => setShowOnlySelected(!showOnlySelected)}
                    />
                    <span style={{ paddingLeft: 4, paddingRight: 4 }}>
                        Show only selected
                    </span>
                    <button onClick={() => prevState()}>Prev State</button>
                    <button onClick={() => nextState()}>Next State</button>
                    <span style={{ paddingLeft: 4, paddingRight: 4 }}>
                        {stateLabel}
                    </span>
                    <input
                        placeholder="Search Class ID"
                        onChange={(e) => setNavigateTo(e.target.value)}
                        value={navigateTo}
                    ></input>
                    <button
                        onClick={() => {
                            navigate(navigateTo);
                        }}
                    >
                        Navigate
                    </button>
                    {navHistory.map((item) => (
                        <span style={{ paddingLeft: 4 }} key={item}>
                            <button onClick={() => zoomTo(fitView, [item])}>
                                {item}
                            </button>
                            <button
                                onClick={() =>
                                    setNavHistory(
                                        navHistory.filter((i) => i !== item),
                                    )
                                }
                            >
                                X
                            </button>
                        </span>
                    ))}
                </div>
                <div>
                    <span>
                        {preparedStates[stateIdx].appliedRules.join(', ')}
                    </span>
                </div>
            </Panel>
        </ReactFlow>
    );
};

function rootComponent() {
    return (
        <ReactFlowProvider>
            <LayoutFlow />
        </ReactFlowProvider>
    );
}

const rootElement = document.getElementById('ui');
if (rootElement === null) {
    throw new Error('Root element not found');
}
const root = createRoot(rootElement);
root.render(rootComponent());
