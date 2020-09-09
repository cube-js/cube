# Slack Vibe 🎉 by Cube.js

*Slack Vibe* is an open source dashboard of public activity in a Slack workspace of an open community or a private team.

*Slack Vibe* is is created and powered by [Cube.js](https://cube.dev), an open source analytical data access layer for modern web applications.

## Live demo

Visit [slack-vibe.cubecloudapp.dev](https://slack-vibe.cubecloudapp.dev) for a live demo with data from Cube.js [community Slack](https://cubejs-community.herokuapp.com). 

## Deploying to Heroku

You can deploy *Slack Vibe* to Heroku.

* Sign up or log in to [Heroku](https://id.heroku.com/login).
* Open the [magic link](https://dashboard.heroku.com/new?template=https://github.com/cube-js/cube.js/tree/heroku/slack-vibe/).
* Configure your deployment: enter application name, choose a region, click "Deploy app".

## Running with Docker

You can run a pre-built Docker image of *Slack Vibe*.

* Run `docker run -p 4000:4000 cubejs/slack-vibe:0.1.0` to start the application.
* Open [localhost:4000](http://localhost:4000) in your browser.

You can also use provided [Dockerfile](./Dockerfile) to build your own image.

## Running locally

You can build and run Slack Vibe on your local machine.

* Run `npm install`, then run `npm run dev` to start the back-end application.
* Run `npm install`, then run `npm run dev` in `frontend` folder to start the front-end application.
* Open [localhost:3000](http://localhost:3000) in your browser.

## Running in production

You can build and run Slack Vibe on a remote server or on a cloud platform of your preference.

* Run `npm install`, then run `npm run build` in `frontend` folder to build the front-end application.
* Run `npm install`, then run `npm start` to start the back-end application.
* Open [localhost:4000](http://localhost:4000) in your browser.

## Database

Slack Vibe stores data in the `db.sqlite` file managed by an embedded SQLite database. Remove this file to clear the data.