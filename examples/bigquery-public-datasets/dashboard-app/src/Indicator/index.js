import React from 'react'
import PropTypes from 'prop-types'
import styles from './Indicator.module.css'

const baselineTooltip = 'The baseline is the median value, for the corresponding day of the week, during the 5-week period from Jan 3 to Feb 6, 2020'

export default class Indicator extends React.Component {
    render() {
        const i = this.props.indicator

        return (
            <div className={styles.indicator}>
                <div className={styles.name}>{i.name}</div>
                <div className={styles.metric}>{i.formatValue(i.value)}</div>
                <div className={styles.footnote}>compared to <span title={baselineTooltip} className={styles.tooltip}>baseline</span></div>
            </div>
        )
    }
}

Indicator.propTypes = {
    indicator: PropTypes.object
}