import { iterations } from './iterations';
import { createRoot } from 'react-dom/client';
import ELK from 'elkjs/lib/elk.bundled.js';
import React, { useCallback, useState, useEffect } from 'react';
import ReactFlow, {
    ReactFlowProvider,
    Panel,
    useNodesState,
    useEdgesState,
    useReactFlow,
} from 'reactflow';

import 'reactflow/dist/style.css';

const data = { nodes: iterations[0].nodes, edges: iterations[0].edges, combos: iterations[0].combos };
const sizeByNode = (n) => [60 + n.label.length * 5, 30];
const toGroupNode = (n) => ({ ...n, type: 'group', data: { label: n.label }, position: { x: 0, y: 0 } });
const toRegularNode = (n) => ({
    ...n,
    extent: 'parent',
    parentNode: n.comboId,
    data: { label: n.label },
    position: { x: 0, y: 0 },
    style: { width: sizeByNode(n)[0], height: sizeByNode(n)[1]},
    draggable: false,
    connectable: false
});
const toEdge = (n) => ({
    ...n,
    id: `${n.source}->${n.target}`,
    style: n.source.match(new RegExp(`^${n.target}-`)) ? { stroke:  '#f00', 'stroke-width': 10 } : undefined
});
const initialNodes = data.combos.map(toGroupNode).concat(data.nodes.map(toRegularNode));
const initialEdges = data.edges.map(toEdge);

const elk = new ELK();

function layout(options, nodes, edges, setNodes, setEdges, fitView) {
    const defaultOptions = {
        'elk.algorithm': 'layered',
        'elk.layered.spacing.nodeNodeBetweenLayers': 100,
        'elk.spacing.nodeNode': 80,
        'org.eclipse.elk.hierarchyHandling': 'INCLUDE_CHILDREN',
        'elk.direction': 'DOWN'
    };
    const layoutOptions = {...defaultOptions, ...options};

    nodes.forEach(n => {
        if (n.style && n.style.width && n.style.height) {
            n.width = n.style.width;
            n.height = n.style.height;
        }
    })
    const groupNodes = nodes.filter((node) => node.type === 'group').map(node => ({[node.id]: node})).reduce((acc, val) => ({...acc, ...val}), {});
    nodes.filter((node) => node.type !== 'group').forEach((node) => groupNodes[node.parentNode] = {
        ...groupNodes[node.parentNode],
        children: (groupNodes[node.parentNode]?.children || []).concat(node)
    });

    const graph = {
        id: 'root',
        layoutOptions: layoutOptions,
        children: Object.keys(groupNodes).map(key => groupNodes[key]),
        edges: edges,
    };

    return elk.layout(graph).then(({children}) => {
        // By mutating the children in-place we saves ourselves from creating a
        // needless copy of the nodes array.
        const flattenChildren = [];

        children.forEach((node) => {
            node.position = {x: node.x, y: node.y};
            node.style = { ...node.style, width: node.width, height: node.height};
            flattenChildren.push(node);
            node.children.forEach(child => {
                child.position = {x: child.x, y: child.y};
                flattenChildren.push(child);
            });
            delete node.children;
        });

        setNodes(flattenChildren);
        setEdges(edges);
        window.requestAnimationFrame(() => {
            fitView();
        });
        return flattenChildren;
    });
}

const highlightColor = 'rgba(170,255,170,0.71)';

const useLayoutedElements = () => {
    const { getNodes, setNodes, getEdges, setEdges, fitView } = useReactFlow();
    const [iteration, setIteration] = useState(0);

    const prevIter = () => {
        if (iteration === 0) {
            return;
        }
        let nodes = getNodes();
        let edges = getEdges();
        const toRemove = iterations[iteration];
        let toRemoveNodeIds = toRemove.nodes.concat(toRemove.combos).map((n) => n.id);
        let toRemoveEdgeIds = toRemove.edges.map((n) => toEdge(n).id);
        nodes = nodes.filter((n) => !toRemoveNodeIds.includes(n.id));
        edges = edges.filter((n) => !toRemoveEdgeIds.includes(n.id));
        nodes = nodes.concat((toRemove.removedCombos || []).map(toGroupNode));
        nodes = nodes.concat((toRemove.removedNodes || []).map(toRegularNode));
        const edgeMap = (toRemove.removedEdges || []).map(toEdge).reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        edges = edges.concat(Object.keys(edgeMap).map(key => edgeMap[key]));
        const toHighlight = iterations[iteration - 1];
        const toHighlightNodeIds = toHighlight.nodes.concat(toHighlight.combos).map((n) => n.id);
        nodes = nodes.map(n => ({...n, style: { ...n.style, 'background-color': toHighlightNodeIds.includes(n.id) ? highlightColor : undefined }}));
        setIteration(iteration - 1);
        layout({}, nodes, edges, setNodes, setEdges, fitView);
        // (toHighlight.nodes || []).forEach((n) => graph.setItemState(graph.findById(n.id), 'justAdded', true));
    }

    const nextIter = () => {
        if (iteration === iterations.length - 1) {
            return;
        }
        let nodes = getNodes();
        let edges = getEdges();
        // const nodes = graph.getNodes();
        // nodes.forEach((node) => graph.setItemState(node, 'justAdded', false));
        setIteration(iteration + 1);
        const toAdd = iterations[iteration + 1];
        let toRemoveNodeIds = toAdd.removedNodes.concat(toAdd.removedCombos).map((n) => n.id);
        let toRemoveEdgeIds = toAdd.removedEdges.map((n) => toEdge(n).id);
        nodes = nodes.filter((n) => !toRemoveNodeIds.includes(n.id));
        edges = edges.filter((n) => !toRemoveEdgeIds.includes(n.id));
        nodes = nodes.map(n => ({...n, style: { ...n.style, 'background-color': undefined }}));
        nodes = nodes.concat(
            (toAdd.combos || []).map(toGroupNode).concat((toAdd.nodes || []).map(toRegularNode))
                .map((n) => ({...n, style: { ...n.style, 'background-color': highlightColor }}))
        );
        const edgeMap = (toAdd.edges || []).map(toEdge).reduce((acc, val) => ({ ...acc, [val.id]: val }), {});
        edges = edges.concat(Object.keys(edgeMap).map(key => edgeMap[key]));

        layout({}, nodes, edges, setNodes, setEdges, fitView);
    };

    const getLayoutedElements = useCallback((options) => {
        let nodes = getNodes();
        let edges = getEdges();

        layout(options, nodes, edges, setNodes, setEdges, fitView);
    }, []);

    return { getLayoutedElements, prevIter, nextIter, iteration };
};

const zoomTo = (fitView, classId) => {
    if (!classId) {
        return;
    }
    fitView({ duration: 600, nodes: [{ id: `c${classId}`}]});
}

const LayoutFlow = () => {
    const [nodes, , onNodesChange] = useNodesState(initialNodes);
    const [edges, , onEdgesChange] = useEdgesState(initialEdges);
    const { getLayoutedElements, prevIter, nextIter, iteration } = useLayoutedElements();
    const { fitView } = useReactFlow();

    const [navigateTo, setNavigateTo] = useState('');
    const [navHistory, setNavHistory] = useState([]);

    useEffect(() => {
        setTimeout(() => {
            getLayoutedElements({});
        }, 100);
    }, []);

    return (
        <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            fitView
            minZoom={0.01}
            onlyRenderVisibleElements
        >
            <Panel position="top-left">
                <div>
                    <button onClick={() => prevIter()}>Prev Iter</button>
                    <button onClick={() => nextIter()}>Next Iter</button>
                    <span style={{ paddingLeft: 4, paddingRight: 4 }}>Iteration #{iteration + 1} / {iterations.length}</span>
                    <input placeholder="Search Class ID" onChange={(e) => setNavigateTo(e.target.value)} value={navigateTo}></input>
                    <button onClick={() => {
                        zoomTo(fitView, navigateTo);
                        if (!navHistory.includes(navigateTo)) {
                            setNavHistory(navHistory.concat(navigateTo));
                        }
                    }}>
                        Navigate
                    </button>
                    {
                        navHistory.map(item => (
                            <span style={{ paddingLeft: 4 }} key={item}>
                        <button onClick={() => zoomTo(fitView, item)}>{item}</button>
                        <button onClick={() => setNavHistory(navHistory.filter(i => i !== item))}>X</button>
                    </span>
                        ))
                    }
                </div>
                <div>
                    <span>{iterations[iteration].appliedRules.join(', ')}</span>
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
