import { formatLongNumber, formatSignedPercent } from "../format"

const confirmedCasesIndicator = {
    key: 'confirmedCases',
    name: 'Confirmed cases',
    color: 'rgb(0, 0, 0)',
    value: 0,
    formatValue: value => formatLongNumber(value),
    isPercent: false,
    measures: []
}

const retailIndicator = {
    key: 'retail',
    name: 'Retail & recreation',
    color: 'rgb(24, 103, 210)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'Stay at home requirements',
            key: 'stayAtHomeRequirements'
        }
    ]
}

const groceryIndicator = {
    key: 'grocery',
    name: 'Grocery & pharmacy',
    color: 'rgb(20, 158, 176)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'Stay at home requirements',
            key: 'stayAtHomeRequirements'
        }
    ]
}

const parkIndicator = {
    key: 'park',
    name: 'Parks',
    color: 'rgb(23, 128, 56)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'Stay at home requirements',
            key: 'stayAtHomeRequirements'
        },
        {
            name: 'Restrictions on gatherings',
            key: 'restrictionsOnGatherings'
        }
    ]
}

const transitIndicator = {
    key: 'transit',
    name: 'Transit stations',
    color: 'rgb(208, 25, 132)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'Close public transit',
            key: 'closePublicTransit'
        },
        {
            name: 'Restrictions on internal movement',
            key: 'restrictionsOnInternalMovement'
        }
    ]
}

const workplaceIndicator = {
    key: 'workplace',
    name: 'Workplaces',
    color: 'rgb(212, 110, 13)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'Workplace closing',
            key: 'workplaceClosing'
        }
    ]
}

const residentialIndicator = {
    key: 'residential',
    name: 'Residential',
    color: 'rgb(132, 49, 206)',
    value: 0,
    formatValue: value => formatSignedPercent(value),
    isPercent: true,
    measures: [
        {
            name: 'School closing',
            key: 'schoolClosing'
        },
        {
            name: 'Restrictions on gatherings',
            key: 'restrictionsOnGatherings'
        },
        {
            name: 'Stay at home requirements',
            key: 'stayAtHomeRequirements'
        }
    ]
}

export const defaultIndicators = [
    confirmedCasesIndicator,
    retailIndicator,
    groceryIndicator,
    parkIndicator,
    transitIndicator,
    workplaceIndicator,
    residentialIndicator
]