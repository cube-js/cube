import { countryCodeEmoji } from 'country-code-emoji'

const countryCodeMap = {
  'EN': 'US',
  'JA': 'JP',
  'ZH': 'CN',
  'FA': 'IR',
  'UK': 'UA',
  'KO': 'KR',
  'CS': 'CZ',
  'HE': 'IL',
  'EL': 'GR',
  'DA': 'DK',
  'HI': 'IN',
  'AR': 'EG',
  'SV': 'SE',
}

export function getEmoji(region) {
  let code = countryCodeMap[region] || region

  return countryCodeEmoji(code)
}