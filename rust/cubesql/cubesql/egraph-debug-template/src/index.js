import G6 from '@antv/g6';
import ReactDOM from 'react-dom';
import React, { useState } from 'react';
import { createRoot } from 'react-dom/client';
import { iterations } from './iterations';

const data = { nodes: iterations[0].nodes, edges: iterations[0].edges, combos: iterations[0].combos };

const prevIter = (iteration, setIteration) => {
    if (iteration === 0) {
        return;
    }
    const toRemove = iterations[iteration];
    (toRemove.edges || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toRemove.nodes || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toRemove.combos || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toRemove.removedCombos || []).forEach((n) => graph.addItem('combo', { ...n }));
    (toRemove.removedNodes || []).forEach((n) => {
        graph.addItem('node', { ...n, size: sizeByNode(n) });
    });
    (toRemove.removedEdges || []).forEach((n) => graph.addItem('edge', { ...n }));
    setIteration(iteration - 1);
    iteration -= 1;
    const toHighlight = iterations[iteration];
    (toHighlight.nodes || []).forEach((n) => graph.setItemState(graph.findById(n.id), 'justAdded', true));
    setTimeout(() => {
        graph.updateLayout();
    }, 1000);
};

const sizeByNode = (n) => [60 + n.label.length * 5, 30];

data.nodes.forEach(n => n.size = sizeByNode(n));

const nextIter = (iteration, setIteration) => {
    if (iteration === iterations.length - 1) {
        return;
    }
    const nodes = graph.getNodes();
    nodes.forEach((node) => graph.setItemState(node, 'justAdded', false));
    setIteration(iteration + 1);
    iteration += 1;
    const toAdd = iterations[iteration];
    (toAdd.removedEdges || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toAdd.removedNodes || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toAdd.removedCombos || []).forEach((n) => graph.removeItem(graph.findById(n.id)));
    (toAdd.combos || []).forEach((n) => graph.addItem('combo', { ...n }));
    (toAdd.nodes || []).forEach((n) => {
        graph.addItem('node', { ...n, size: sizeByNode(n) });
        graph.setItemState(graph.findById(n.id), 'justAdded', true);
    });
    (toAdd.edges || []).forEach((n) => graph.addItem('edge', { ...n }));

    setTimeout(() => {
        graph.updateLayout();
    }, 1000);
};

const navigateToClass = (classId) => {
    if(!classId) {
        return;
    }
    graph.focusItem(graph.findById(classId + ''), true, {
        easing: 'easeCubic',
        duration: 500,
    });
};

const UI = () => {
    const [iteration, setIteration] = useState(0);
    const [navigateTo, setNavigateTo] = useState('');
    const [navHistory, setNavHistory] = useState([]);
    return (<div>
        <div>
            <button onClick={() => prevIter(iteration, setIteration)}>Prev Iter</button>
            <button onClick={() => nextIter(iteration, setIteration)}>Next Iter</button>
            <span style={{ paddingLeft: 4, paddingRight: 4 }}>Iteration #{iteration + 1} / {iterations.length}</span>
            <input placeholder="Search Class ID" onChange={(e) => setNavigateTo(e.target.value)} value={navigateTo}></input>
            <button onClick={() => {
                navigateToClass(parseInt(navigateTo));
                if (!navHistory.includes(navigateTo)) {
                    setNavHistory(navHistory.concat(navigateTo));
                }
            }}>
                Navigate
            </button>
            {
                navHistory.map(item => (
                    <span style={{ paddingLeft: 4 }} key={item}>
                    <button onClick={() => navigateToClass(parseInt(item))}>{item}</button>
                    <button onClick={() => setNavHistory(navHistory.filter(i => i !== item))}>X</button>
                </span>
                ))
            }
        </div>
        <div>
            <span>{iterations[iteration].appliedRules.join(', ')}</span>
        </div>
    </div>)
}

const rootElement = document.getElementById('ui');
const root = createRoot(rootElement);
root.render(<UI />);

let fixSelectedItems = {
    fixAll: true,
    fixState: 'yourStateName', // 'selected' by default
};

const vw = Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0);
const vh = Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0);
const width = (vw || container.scrollWidth) - 20;
const height = (vh || container.scrollHeight || 500) - 30;
const graph = new G6.Graph({
    container: 'container',
    width,
    height: height - 50,
    fitView: true,
    fitViewPadding: 30,
    animate: true,
    groupByTypes: false,
    modes: {

        default: [
            'drag-combo',
            // 'drag-node',
            'drag-canvas',
            {
                type: 'zoom-canvas',
                fixSelectedItems,
            },
            {
                type: 'collapse-expand-combo',
                relayout: false,
            },
            'activate-relations'
        ],
    },
    layout: {
        type: 'dagre',
        sortByCombo: true,
        ranksep: 20,
        nodesep: 10,
    },
    nodeStateStyles: {
        justAdded: {
            fill: '#c3e3ff',
            stroke: '#aaa',
        },
    },
    defaultNode: {
        size: [60, 30],
        type: 'rect',
        anchorPoints: [[0.5, 0], [0.5, 1]],
        style: {
            fill: '#FDE1CE',
            stroke: '#aaa',
        },
    },
    defaultEdge: {
        type: 'line',
    },
    defaultCombo: {
        type: 'rect',
        style: {
            fillOpacity: 0.1,
            fill: '#C4E3B2',
            stroke: '#C4E3B2',
        },
    },
});
graph.data(data);
graph.render();
const nodes = graph.getNodes();
nodes.forEach((node) => graph.setItemState(node, 'justAdded', true));

if (typeof window !== 'undefined')
    window.onresize = () => {
        if (!graph || graph.get('destroyed')) return;
        if (!container || !container.scrollWidth || !container.scrollHeight) return;
        graph.changeSize(container.scrollWidth, container.scrollHeight - 30);
    };

