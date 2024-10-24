import states from './states.json';
import { createRoot } from 'react-dom/client';
import ELK from 'elkjs/lib/elk.bundled.js';
import React, { useCallback, useState, useEffect, useMemo } from 'react';
import ReactFlow, {
    ReactFlowProvider,
    Panel,
    useNodesState,
    useEdgesState,
    useReactFlow,
    Handle,
    Position,
} from 'reactflow';

import 'reactflow/dist/style.css';

// First is initial state
const totalIterations = states.length - 1;
const data = {
    nodes: states[0].nodes,
    edges: states[0].edges,
    combos: states[0].combos,
};
const sizeByNode = (n) => [60 + n.label.length * 5, 30];
const toGroupNode = (n) => ({
    ...n,
    type: 'group',
    data: { label: n.label },
    position: { x: 0, y: 0 },
    width: 200,
    height: 200,
});
const toRegularNode = (n) => ({
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
const toEdge = (n) => ({
    ...n,
    id: `${n.source}->${n.target}`,
    style:
        n.source.indexOf(`${n.target}-`) === 0
            ? { stroke: '#f00', 'stroke-width': 10 }
            : undefined,
});
const initialNodes = data.combos
    .map(toGroupNode)
    .concat(data.nodes.map(toRegularNode));
const initialEdges = data.edges.map(toEdge);

function layout(
    options,
    nodes,
    edges,
    setNodes,
    setEdges,
    fitView,
    navHistory,
    showOnlySelected,
) {
    const defaultOptions = {
        'elk.algorithm': 'layered',
        'elk.layered.spacing.nodeNodeBetweenLayers': 100,
        'elk.spacing.nodeNode': 80,
        'org.eclipse.elk.hierarchyHandling': 'INCLUDE_CHILDREN',
        'elk.direction': 'DOWN',
    };
    const layoutOptions = { ...defaultOptions, ...options };

    nodes.forEach((n) => {
        if (n.style && n.style.width && n.style.height) {
            n.width = n.style.width;
            n.height = n.style.height;
        }
    });
    nodes = nodes.filter((n) => !isHiddenNode(showOnlySelected, navHistory, n));
    edges = edges.filter((e) => !isHiddenEdge(showOnlySelected, navHistory, e));
    const groupNodes = nodes
        .filter((node) => node.type === 'group')
        .map((node) => ({ [node.id]: node }))
        .reduce((acc, val) => ({ ...acc, ...val }), {});
    nodes
        .filter((node) => node.type !== 'group')
        .forEach(
            (node) =>
                (groupNodes[node.parentNode] = {
                    ...groupNodes[node.parentNode],
                    children: (
                        groupNodes[node.parentNode]?.children || []
                    ).concat(node),
                }),
        );

    const graph = {
        id: 'root',
        layoutOptions: layoutOptions,
        children: Object.keys(groupNodes).map((key) => groupNodes[key]),
        edges: edges,
    };

    const elk = new ELK();
    return elk.layout(graph).then(({ children }) => {
        // By mutating the children in-place we saves ourselves from creating a
        // needless copy of the nodes array.
        const flattenChildren = [];

        children.forEach((node) => {
            node.position = { x: node.x, y: node.y };
            node.style = {
                ...node.style,
                width: node.width,
                height: node.height,
            };
            flattenChildren.push(node);
            node.children.forEach((child) => {
                child.position = { x: child.x, y: child.y };
                flattenChildren.push(child);
            });
            delete node.children;
        });

        setNodes(flattenChildren);
        setEdges(edges);
        window.requestAnimationFrame(() => {
            if (navHistory?.length) {
                setTimeout(() => {
                    zoomTo(fitView, navHistory);
                }, 500);
            } else {
                fitView();
            }
        });
        return flattenChildren;
    });
}

const highlightColor = 'rgba(170,255,170,0.71)';
const selectColor = 'rgba(170,187,255,0.71)';

const zoomTo = (fitView, classId) => {
    if (!classId) {
        return;
    }
    fitView({ duration: 600, nodes: classId.map((id) => ({ id: `c${id}` })) });
};

function isHiddenNode(showOnlySelected, navHistory, n) {
    return (
        showOnlySelected &&
        navHistory.indexOf(
            n.id.replace('c', '').replace(/^(\d+)-.*$/, '$1'),
        ) === -1
    );
}

const nodeStyles = (nodes, navHistory, showOnlySelected) => {
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

function isHiddenEdge(showOnlySelected, navHistory, e) {
    return (
        showOnlySelected &&
        (navHistory.indexOf(e.source.replace(/^(\d+)(-?).*$/, '$1')) === -1 ||
            navHistory.indexOf(e.target.replace(/^(\d+)(-?).*$/, '$1')) === -1)
    );
}

const edgeStyles = (edges, navHistory, showOnlySelected) => {
    return edges.map((e) => {
        return {
            ...e,
            hidden: isHiddenEdge(showOnlySelected, navHistory, e),
        };
    });
};

const splitLabel = (label) => {
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
    ({ navigate /*, nodes*/ }) =>
    ({ data: { label } }) => {
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

const LayoutFlow = () => {
    const [{ preNodes, preEdges }, setPreNodesEdges] = useState({
        preNodes: initialNodes,
        preEdges: initialEdges,
    });
    const [nodes, setNodes, onNodesChange] = useNodesState(
        JSON.parse(JSON.stringify(initialNodes)),
    );
    const [edges, setEdges, onEdgesChange] = useEdgesState(
        JSON.parse(JSON.stringify(initialEdges)),
    );
    const [stateIdx, setStateIdx] = useState(0);
    const { fitView } = useReactFlow();

    const [navigateTo, setNavigateTo] = useState('');
    const [navHistory, setNavHistory] = useState([]);
    const [showOnlySelected, setShowOnlySelected] = useState(false);

    const prevState = () => {
        if (stateIdx === 0) {
            return;
        }
        let newNodes = preNodes;
        let newEdges = preEdges;
        const toRemove = states[stateIdx];
        let toRemoveNodeIds = toRemove.nodes
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
        const edgeMap = (toRemove.removedEdges || [])
            .map(toEdge)
            .reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        newEdges = newEdges.concat(
            Object.keys(edgeMap).map((key) => edgeMap[key]),
        );
        const toHighlight = states[stateIdx - 1];
        const toHighlightNodeIds = toHighlight.nodes
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
        if (stateIdx === states.length - 1) {
            return;
        }
        let newNodes = preNodes;
        let newEdges = preEdges;
        setStateIdx(stateIdx + 1);
        const toAdd = states[stateIdx + 1];
        let toRemoveNodeIds = toAdd.removedNodes
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
        const edgeMap = (toAdd.edges || [])
            .map(toEdge)
            .reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        newEdges = newEdges.concat(
            Object.keys(edgeMap).map((key) => edgeMap[key]),
        );

        setPreNodesEdges({ preNodes: newNodes, preEdges: newEdges });
    };

    const navigate = useCallback(
        (id) => {
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
        layout(
            {},
            JSON.parse(JSON.stringify(preNodes)),
            JSON.parse(JSON.stringify(preEdges)),
            setNodes,
            setEdges,
            fitView,
            navHistory,
            showOnlySelected,
        );
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
                    <span>{states[stateIdx].appliedRules.join(', ')}</span>
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
const root = createRoot(rootElement);
root.render(rootComponent());
