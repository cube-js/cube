import React from 'react'
import PropTypes from 'prop-types'
import styles from './Header.module.css'

export default class Header extends React.Component {
    render() {
        return (
            <>
                <h1 className={styles.header}>
                    COVID-19 in&nbsp;
                    <span className={styles.selector} onClick={this.props.enableFrame}>{this.props.country || 'your country'}</span>
                    <span className={styles.button} onClick={this.props.changeToRandomCountry}>&#8634;</span>
                </h1>
            </>
        )
    }
}

Header.propTypes = {
    country: PropTypes.string,
    enableFrame: PropTypes.func,
    changeToRandomCountry: PropTypes.func
}