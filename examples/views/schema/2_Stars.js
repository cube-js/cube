view('Stars', {
  // shown: false,

  includes: [
    Stargazers.total,
    Stargazers.repository
  ],

  // dimensions: {
  //   user: {
  //     sql: `${Stargazers.login}`,
  //     type: `string`
  //   }
  // }
});