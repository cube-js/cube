import React, { Component } from 'react';

const generateEventSelectId = (eventSelects) => Math.max( ...eventSelects ) + 1
const resolveGranularity = (visualizationType, state) => {
  // Reset granularity if pie chart selected,
  // but memorized previousily selected
  if (visualizationType === 'pie') {
    return { granularity: null, memorizedGranularity: state.granularity }
  // For the rest of the charts use currently selected granularity,
  // or in case it is null the memorized one
  } else {
    return {
      granularity: (state.granularity || state.memorizedGranularity),
      memorizedGranularity: null
    }
  }
}

class Store {
  constructor(initialData) {
    this.data = initialData
  }

  change(action) {
    const newData = this.reducer(this.data, action);
    this.data = newData;
  }

  reducer(state, action) {
    switch (action.type) {
      case 'REMOVE_EVENT_SELECT':
        const eventSelectsCloned = state.eventSelects.filter(i => i !== action.id)
        return {
          ...state,
          eventSelects: eventSelectsCloned
        }
      case 'ADD_EVENT_SELECT':
        const eventSelects = [...state.eventSelects]
        eventSelects.push(generateEventSelectId(eventSelects))
        return {
          ...state,
          eventSelects
        }
      case 'CHANGE_DATERANGE':
        return {
          ...state,
          dateRange: action.value
        }
      case 'CHANGE_GRANULARITY':
        return {
          ...state,
          granularity: action.value
        }
      case 'CHANGE_VISUALIZATION_TYPE':
        const {
          granularity,
          memorizedGranularity
        } = resolveGranularity(action.value, state)
        return {
          ...state,
          granularity,
          memorizedGranularity,
          visualizationType: action.value
        }
      case 'ADD_DIMENSION':
        return {
          ...state,
          dimensions: [action.value]
        }
      case 'REMOVE_DIMENSION':
        return {
          ...state,
          dimensions: []
        }
      case 'ADD_MEASURE':
        const measures = {...state.measures}
        measures[action.id] = action.value
        return {
          ...state,
          measures
        }
      case 'REMOVE_MEASURE':
        const newMeasures = {...state.measures}
        delete newMeasures[action.id]
        return {
          ...state,
          measures: newMeasures
        }
      default:
        return state
    }
  }
}


const withQueryBuilder = (initialData, WrappedComponent) =>
  class withQueryBuilderComponent extends Component {
    constructor(props) {
      super(props)
      this.store = new Store(initialData)
    }

    onChange(args) {
      this.store.change(args)
      this.forceUpdate();
    }

    render() {
      return <WrappedComponent {...this.store.data} onChange={this.onChange.bind(this)} />
    }
  }

export default withQueryBuilder;
