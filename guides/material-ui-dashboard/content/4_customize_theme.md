---
order: 4
title: "Customize theme"
---

Next, we’re going to use a custom Material UI theme for our dashboard. You can learn more about creating your Material UI theme [here](https://material-ui.com/customization/theming/). You can download the theme from Github here.

```jsx
https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/src/theme
```

Then install Roboto font:

```bash
// npm
npm install typeface-roboto
// yarn
yarn add typeface-roboto
```

Now, we need to add a theme to ThemeProvider in App.js file:

```diff
// ...
- import { makeStyles } from "@material-ui/core/styles";
+ import {makeStyles, ThemeProvider} from "@material-ui/core/styles";
+ import theme from './theme';
+ import 'typeface-roboto'
// ...
const AppLayout = ({children}) => {
  const classes = useStyles();
  return (
+   <ThemeProvider theme={theme}>
      <div className={classes.root}>
        <Header/>
        <div>{children}</div>
      </div>
+   </ThemeProvider>
  );
};
// ...
```
