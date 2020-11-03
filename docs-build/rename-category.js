const renameRules = {
  'Data schema': 'Data Schema',
  'Guides': 'Data Schema',
  'Reference': 'Data Schema',
}

exports.renameCategory = category => renameRules[category] || category;