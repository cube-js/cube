export function formatSignedPercent(number) {
    return `${number > 0 ? '+' : number < 0 ? '–' : ''} ${Math.abs(number)} %`
}

export function formatLongNumber(number) {
    return Math.round(number).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function rgba(rgb, alpha) {
    return rgb.replace(')', `,${alpha})`)
}