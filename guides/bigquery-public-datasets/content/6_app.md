---
order: 6
title: "How to Build an Analytical App"
---

It's worth noting that Cube.js Developer Playground has one more feature to explore.

If you go to the "Dashboard App" tab, you'll be able to generate the code for a front-end application with a dashboard. There're various templates for different frameworks (React and Angular included) and charting libraries there. Still, you can always choose to "create your own," and if you choose a "dynamic" template, you'll be able to compose queries and add charts just like you did.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/43ljijihw21cpknz4i22.png)

However, we'll choose a much simpler way to go from zero to a full-fledged analytical app — we'll grab the code from GitHub:

* first, download this [dashboard-app.zip](https://github.com/cube-js/cube.js/blob/master/examples/bigquery-public-datasets/dashboard-app.zip) file
* unzip it to your `bigquery-public-datasets` folder
* run `yarn` and `yarn start` (or `npm install` and `npm start`)

You should be all set! Navigate to [localhost:3000](http://localhost:3000) and have a look at this app:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/h2ee2ag8q2elit2yis75.png)

Choose your country and take your time to explore the impact of COVID-19 and how mitigation measures correlate with social mobility.

**Let's take Israel.** You can clearly see three waves and the "easing" effect of "stay at home" requirements — after they are introduced, every wave spreads with lesser speed.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/wts71vfluxnwvuhza7u9.png)

**Let's take Germany.** You can see how Germans interact with the rules: after the first "stay at home" requirements are lifted, park activity grows, and after the second "stay at home" requirements are introduced, parks instantly become deserted.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/e63mtd6ocea1f3u7rq0q.png)

**Let's take Singapore.** Obviously enough, you can see Singapore doing a great job containing the virus. The third wave is nearly unexistent.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/cn7fc0ww08xraetp9ikn.png)

**What are your own insights? Please share them in the comments!**

And now, let's explore a few crucial parts of this app to understand better how it works and, more specifically, how it retrieves data from Cube.js API.

First, as you can see from `package.json`, it's obviously a React app created with the `create-react-app` utility. It has an `index.js` as an entry point and the `App` root component.

Second, it references `@cubejs-client/core` and `@cubejs-client/react` packages as dependencies. Here's what you can see in the `api.js` file:

```js
// Let's use Cube.js client library to talk to Cube.js API
import cubejs from '@cubejs-client/core'

// API URL and authentication token are stored in .env file 
const cubejsApi = cubejs(
    process.env.REACT_APP_CUBEJS_TOKEN,
    { apiUrl: `${process.env.REACT_APP_API_URL}/cubejs-api/v1` }
);

// The simplest Cube.js query possible:
// "Hey, Cube.js, give us a list of all countries."
const countriesQuery = {
    dimensions: [ 'Mobility.country' ]
}

export function loadCountries(callback) {
    // cubejsApi.load returns a promise.
    // Once it's resolved, we can get the result.
    // We can even transform it with tablePivot() or chartPivot()
    cubejsApi
        .load(countriesQuery)
        .then(result => {
            const countries = result
                .tablePivot()
                .map(row => row['Mobility.country'])

            callback(countries)
        })
}
```

Believe it or not, that's the bare minimum we should know about working with Cube.js REST API in the front-end apps. You import a client library, you compose your query as a JSON object, you load the result asynchronously, and you do whatever you want with the data.

In this application, the data is visualized with Chart.js, a great data visualization library. However, you can choose any library you're familiar with. And maybe your app will look even better than this one:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/0inga4y2cruq5dvv4qno.png)

And that's all, folks! 🦠 I hope you liked this tutorial 🤗

Here's just a few things you can do in the end:
* go to the [Cube.js repo](https://github.com/cube-js/cube.js/) on GitHub and give it a star ⭐️
* share a link to this tutorial on social media or with a friend 🙋‍♀️
* share your insights about the impact of COVID-19 in Cube.js [community Slack](https://slack.cube.dev)