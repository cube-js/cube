import cubejs from '@cubejs-client/core'
import moment from 'moment'

const cubejsApi = cubejs(
    process.env.REACT_APP_CUBEJS_TOKEN,
    { apiUrl: `${process.env.REACT_APP_API_URL}/cubejs-api/v1` }
);

const countriesQuery = {
    dimensions: [ 'Mobility.country' ]
}

export function loadCountries(callback) {
    cubejsApi
        .load(countriesQuery)
        .then(result => {
            const countries = result
                .tablePivot()
                .map(row => row['Mobility.country'])

            callback(countries)
        })
}

function createMobilityDataQuery(country) {
    return {
        "dimensions": [
            "Mobility.country"
        ],
        "timeDimensions": [
            {
                "dimension": "Mobility.date",
                "granularity": "day",
                "dateRange": [ '2020-01-01', moment().subtract(4, 'day').format('YYYY-MM-DD') ],
            }
        ],
        "measures": [
            "Measures.confirmed_cases",
            "Measures.schoolClosing",
            "Measures.workplaceClosing",
            "Measures.restrictionsOnGatherings",
            "Measures.closePublicTransit",
            "Measures.stayAtHomeRequirements",
            "Measures.restrictionsOnInternalMovement",
            "Mobility.grocery",
            "Mobility.park",
            "Mobility.residential",
            "Mobility.retail",
            "Mobility.transit",
            "Mobility.workplace"
        ],
        "order": {
            "Mobility.date": "asc"
        },
        "filters": [
            {
                "dimension": "Mobility.country",
                "operator": "equals",
                "values": [ country ]
            }
        ],
        limit: 50000
    }
}

export function loadMobilityData(country, callback) {
    cubejsApi
        .load(createMobilityDataQuery(country))
        .then(result => {

            const data = result
                .chartPivot()
                .map(row => ({
                    x: row['x'],
                    mobility: {
                        confirmedCases: row[`${country}, Measures.confirmed_cases`],
                        grocery: Math.round(row[`${country}, Mobility.grocery`]),
                        park: Math.round(row[`${country}, Mobility.park`]),
                        residential: Math.round(row[`${country}, Mobility.residential`]),
                        retail: Math.round(row[`${country}, Mobility.retail`]),
                        transit: Math.round(row[`${country}, Mobility.transit`]),
                        workplace: Math.round(row[`${country}, Mobility.workplace`])
                    },
                    measures: {
                        schoolClosing: row[`${country}, Measures.schoolClosing`],
                        workplaceClosing: row[`${country}, Measures.workplaceClosing`],
                        restrictionsOnGatherings: row[`${country}, Measures.restrictionsOnGatherings`],
                        closePublicTransit: row[`${country}, Measures.closePublicTransit`],
                        stayAtHomeRequirements: row[`${country}, Measures.stayAtHomeRequirements`],
                        restrictionsOnInternalMovement: row[`${country}, Measures.restrictionsOnInternalMovement`]
                    }
                }))

            if (data.length > 0) {
                callback(data)
            }
        })
}