import React from 'react'
import PropTypes from 'prop-types'
import styles from './CountrySelector.module.css'

export default class CountrySelector extends React.Component {
    render() {
        let lastLetter = undefined

        return (
            <div className={styles.selector}>
                <ul className={styles.list}>
                    {this.props.countries.map(country => {
                        let flag = false

                        if (lastLetter !== country[0]) {
                            lastLetter = country[0]
                            flag = true
                        }

                        return (
                            <li key={country}>
                                {flag && <span className={styles.letter}>{lastLetter}</span>}
                                <span className={styles.country} onClick={() => this.props.selectCountry(country)}>{country}</span>
                            </li>
                        )
                    })}
                </ul>
            </div>
        )
    }
}

CountrySelector.propTypes = {
    countries: PropTypes.array,
    selectCountry: PropTypes.func
}