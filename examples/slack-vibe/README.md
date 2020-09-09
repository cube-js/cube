# Slack Vibe 🎉 by Cube.js

*Slack Vibe* is an open source dashboard of public activity in a Slack workspace of an open community or a private team.

*Slack Vibe* is is created and powered by [Cube.js](https://cube.dev), an open source analytical data access layer for modern web applications.

## Live demo

Visit [slack-vibe.cubecloudapp.dev](https://slack-vibe.cubecloudapp.dev) for a live demo with data from Cube.js [community Slack](https://cubejs-community.herokuapp.com). 

## Running with Docker

Run `docker run -p 4000:4000 cubejs/slack-vibe:0.1.0` to pull a pre-built image, then open [localhost:4000](http://localhost:4000).

Use provided [Dockerfile](./Dockerfile) to build your own image.

## Running locally

First, run `npm run dev` in this folder to start the back-end application.

Second, run `npm run dev` in `frontend` folder to start the front-end application.

Then, open `http://localhost:3000` in your browser.

## Running in production

First, run `npm run build` in `frontend` folder to build the front-end application.

Second, run `npm start`.

Then, open `http://localhost:4000` in your browser.

## Database

Slack Vibe stores data in the `db.sqlite` file managed by an embedded SQLite database. Remove this file to clear the data.