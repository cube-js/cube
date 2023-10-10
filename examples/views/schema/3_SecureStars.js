cube('AirbyteStargazers', {
  shown: false,
  extends: Stargazers,

  sql: `
    SELECT *
    FROM (${Stargazers.sql()})
    WHERE repository = 'airbytehq/airbyte'
  `
});

cube('CubeStargazers', {
  shown: false,
  extends: Stargazers,

  sql: `
    SELECT *
    FROM (${Stargazers.sql()})
    WHERE repository = 'cube-js/cube.js'
  `
});

view('AirbyteStars', {
  shown: COMPILE_CONTEXT.securityContext.scope == 'airbyte',

  includes: [
    AirbyteStargazers.total
  ]
});

view('CubeStars', {
  shown: COMPILE_CONTEXT.securityContext.scope == 'cube',

  includes: [
    CubeStargazers.total
  ]
});