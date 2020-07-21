
const Action = {
  test: function () {
    alert('test')
  },
  Query: {
    dashboardItems () {
      const dashboardItems = Action.Mutation.getDashboardItems()
      return dashboardItems.map(Action.Mutation.toApolloItem)
    },

    dashboardItem (_, { id }) {
      const dashboardItems = Action.Mutation.getDashboardItems()
      return Action.Mutation.toApolloItem(dashboardItems.find(i => i.id.toString() === id))
    }
  },
  Mutation: {
    createDashboardItem: (item) => {
      const dashboardItems = Action.Mutation.getDashboardItems()
      item = { ...item, id: Action.Mutation.getNextId(), layout: JSON.stringify({}) }
      dashboardItems.push(item)
      Action.Mutation.setDashboardItems(dashboardItems)

      console.info('list---0>', Action.Mutation.getDashboardItems())
    },
    updateDashboardItem: (_, { id, input: { ...item } }) => {
      const dashboardItems = Action.Mutation.getDashboardItems()
      item = Object.keys(item)
          .filter(k => !!item[k])
          .map(k => ({
            [k]: item[k]
          }))
          .reduce((a, b) => ({ ...a, ...b }), {})
      const index = dashboardItems.findIndex(i => i.id.toString() === id)
      dashboardItems[index] = { ...dashboardItems[index], ...item }
      Action.Mutation.setDashboardItems(dashboardItems)
      return Action.Mutation.toApolloItem(dashboardItems[index])
    },
    deleteDashboardItem: ({ id }) => {
      const dashboardItems = Action.Mutation.getDashboardItems()
      const index = dashboardItems.findIndex(i => i.id.toString() === id)
      const [removedItem] = dashboardItems.splice(index, 1)
      Action.Mutation.setDashboardItems(dashboardItems)
      return Action.Mutation.toApolloItem(removedItem)
    },
    getDashboardItems: () => {
      console.info('dashboardItems--->', window.localStorage.getItem('dashboardItems'))
      try {
        return JSON.parse(window.localStorage.getItem('dashboardItems')) ||
            []
      } catch (e) {
        console.error(e)
        return []
      }
    },
    deleteDashboardItems: () => {
      return window.localStorage.setItem('dashboardItems', null)
    },
    setDashboardItems: items =>
        window.localStorage.setItem('dashboardItems', JSON.stringify(items)),
    getNextId: () => {
      const currentId =
          parseInt(window.localStorage.getItem('dashboardIdCounter'), 10) || 1
      window.localStorage.setItem('dashboardIdCounter', currentId + 1)
      return currentId.toString()
    },
    toApolloItem: (i) => ({ ...i, __typename: 'DashboardItem' })

  }

}

export default Action
