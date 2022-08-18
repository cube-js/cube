import React from 'react'
import { Layout } from 'antd'
import Header from "../Header"
import Indicator from "../Indicator"
import styles from "./App.module.css"
import { loadCountries, loadMobilityData } from "../api"
import CountrySelector from "../CountrySelector"
import Chart from "../Chart"
import { defaultCountries } from "./countries"
import { defaultIndicators } from "./indicators"
import Description from "../Description"
import PropTypes from "prop-types"

export default class App extends React.Component {
    constructor(props) {
        super(props)

        this.state = {
            isCountrySelectorEnabled: false,
            areIndicatorsUpdated: false,
            countries: [],
            country: props.country || defaultCountries[0],
            indicators: defaultIndicators,
            data: []
        }
    }

    getRandomStartingCountry() {
        let index = Math.ceil(Math.random() * defaultCountries.length)

        return defaultCountries[index]
    }

    componentDidMount() {
        this.updateCountries()
        this.updateIndicators()
    }

    updateCountries() {
        loadCountries(countries => {
            this.setState({ countries })
        })
    }

    updateIndicators() {
        loadMobilityData(this.state.country, data => {
            const mostRecentRow = data.reduce((recent, row) => recent.x < row.x ? row : recent)

            this.setState({
                indicators: defaultIndicators.map(i => ({
                    ...i,
                    value: mostRecentRow.mobility[i.key]
                })),
                data,
                areIndicatorsUpdated: true
            })
        })
    }

    enableCountrySelectorFrame() {
        this.setState({
            isCountrySelectorEnabled: true
        })
    }

    changeToRandomCountry() {
        let index = Math.ceil(Math.random() * this.state.countries.length)

        this.selectCountry(this.state.countries[index])
    }

    selectCountry(country) {
        this.setState({
            country,
            isCountrySelectorEnabled: false,
            areIndicatorsUpdated: false
        }, this.updateIndicators)

        window.location.hash = country
        window.scroll(0, 0)
    }

    render() {
        return (
            <div className="App">
                {this.state.isCountrySelectorEnabled
                    ? this.renderCountrySelector()
                    : this.renderReports()
                }
            </div>
        )
    }

    renderCountrySelector() {
        return (
            <CountrySelector
                countries={this.state.countries}
                selectCountry={this.selectCountry.bind(this)}
            />
        )
    }

    renderReports() {
        return (
          <Layout>
              <Layout.Content style={{ padding: '7.5vh 7.5vw' }}>
                  <Header
                    country={this.state.country}
                    enableFrame={() => this.enableCountrySelectorFrame()}
                    changeToRandomCountry={() => this.changeToRandomCountry()}
                  />

                  <div className={styles.footnotes}>
                      <div>Charts in color provide insights into changes in community mobility due to measures implemented
                          to mitigate COVID-19.</div>
                      <div>Gray rectangles show the time frames of relevant measures.</div>
                  </div>

                  {this.state.areIndicatorsUpdated && this.state.indicators.map(i => (
                    <div key={i.key} className={styles.indicator}>
                        <Indicator indicator={i} />
                        <Chart indicator={i} data={this.state.data} />
                        <Description indicator={i} data={this.state.data} />
                    </div>
                  ))}

                  {!this.state.areIndicatorsUpdated && (
                    <div className={styles.loader}>Loading data...</div>
                  )}
              </Layout.Content>
          </Layout>
        )
    }
}

App.propTypes = {
    country: PropTypes.string
}