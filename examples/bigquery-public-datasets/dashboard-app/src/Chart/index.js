import React from 'react'
import PropTypes from 'prop-types'
import styles from './Chart.module.css'
import { Line } from "react-chartjs-2"
import { formatSignedPercent, rgba } from "../format"

export default class Chart extends React.Component {
    render() {
        const i = this.props.indicator

        const data = {
            labels: this.props.data.map(row => row.x),
            datasets: [
                {
                    backgroundColor: rgba(i.color, 0.2),
                    borderColor: i.color,
                    borderWidth: 2,
                    data: this.props.data.map(row => row.mobility[i.key]),
                    label: i.name,
                    pointRadius: 0,
                    xAxisID: 'time',
                    yAxisID: i.isPercent ? 'mobility' : 'cases'
                },
                ...(i.measures.map(m => ({
                    backgroundColor: rgba('rgb(0, 0, 0)', 0.05),
                    borderWidth: 0,
                    data: this.props.data.map(row => row.measures[m.key] * 1000),
                    label: m.name,
                    pointRadius: 0,
                    xAxisID: 'time',
                    yAxisID: 'measures'
                })))
            ]
        }

        const mobilityAxis = {
            id: 'mobility',
            type: 'linear',
            ticks: {
                suggestedMin: -100,
                suggestedMax: 100,
                maxTicksLimit: 5,
                callback: value => value === 0 ? 'Baseline' : formatSignedPercent(value),
                fontColor: 'rgba(0, 0, 0, 0.5)',
                fontSize: 13.3,
                padding: 10
            },
            gridLines: {
                tickMarkLength: 0
            }
        }

        const measuresAxis = {
            id: 'measures',
            type: 'linear',
            position: 'right',
            ticks: {
                display: false,
                min: 0,
                max: 1
            },
            gridLines: {
                display: false,
                tickMarkLength: 0
            }
        }

        const casesAxis = {
            id: 'cases',
            type: 'linear',
            ticks: {
                suggestedMin: 0,
                maxTicksLimit: 5,
                fontColor: 'rgba(0, 0, 0, 0.5)',
                fontSize: 13.3,
                padding: 10
            },
            gridLines: {
                tickMarkLength: 0
            }
        }

        const options = {
            legend: {
                display: false
            },
            scales: {
                xAxes: [
                    {
                        id: 'time',
                        type: 'time',
                        time: {
                            unit: 'month',
                            displayFormats: {
                                month: 'MMM'
                            }
                        },
                        gridLines: {
                            display: false
                        },
                        ticks: {
                            fontColor: 'rgba(0, 0, 0, 0.5)',
                            fontSize: 13.3
                        }
                    }
                ],
                yAxes: i.isPercent ? [ mobilityAxis, measuresAxis ] : [ casesAxis ]
            },
            tooltips: {
                enabled: false
            },
            hover: {
                mode: null
            },
            animation: {
                duration: 0
            },
            maintainAspectRatio: false
        }

        return (
            <div className={styles.container}>
                <Line data={data} options={options} />
            </div>
        )
    }
}

Chart.propTypes = {
    indicator: PropTypes.object,
    data: PropTypes.array
}