import React from 'react'
import PropTypes from 'prop-types'
import styles from './Description.module.css'

const findTimeIntervals = (data, measure) => data.reduce((intervals, row) => {
    const lastInterval = intervals[intervals.length - 1]

    if (lastInterval === undefined || lastInterval.length === 2) {
        // Looking for the beginning of a new interval
        if (row.measures[measure.key] > 0) {
            intervals.push([ row.x ])
        }
    }
    else if (lastInterval.length === 1) {
        // Looking for the end of the current interval
        if (row.measures[measure.key] === 0) {
            intervals[intervals.length - 1].push(row.x)
        }
    }

    return intervals
}, [])

export default class Description extends React.Component {
    render() {
        const dateOptions = {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
        }

        let intervalCount = 0

        return (
            <div>
                <ul className={styles.list}>
                    {this.props.indicator.measures.map(m => {
                        const intervals = findTimeIntervals(this.props.data, m)
                        intervalCount += intervals.length

                        if (intervals.length === 0) return null

                        const intervalsText = intervals
                            .map(i => i.map(d => new Intl.DateTimeFormat('en-US', dateOptions).format(new Date(d))))
                            .map((i, j) => <span key={j} className={styles.nb}>{`from ${i[0]}${i[1] !== undefined ? ` to ${i[1]}` : ''}`}</span>)
                            .reduce((acc, cur) => [acc, ', then ', cur])

                        return (
                            <li key={m.key}>{m.name} {intervalsText}</li>
                        )
                    })}

                    {this.props.indicator.isPercent && intervalCount === 0 && (
                        <li key='nothing'>No&nbsp;relevant measures in&nbsp;place.</li>
                    )}
                </ul>
            </div>
        )
    }
}

Description.propTypes = {
    indicator: PropTypes.object,
    data: PropTypes.array
}