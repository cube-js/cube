# Stripe Dashboard Example
This example project contains a demo Stripe dashboard built with Cube.js from the [Building a Serverless Stripe Analytics Dashboard](https://cube.dev/blog/building-serverless-stripe-analytics-dashboard/) tutorial.
It uses Recharts for visiualizations.

[Live Demo](http://cubejs-stripe-dashboard-example.s3-website-us-west-2.amazonaws.com/)

## Get started
### 1. Download example & Install dependencies
Clone the repository:

```
git clone git@github.com:statsbotco/cubejs-client.git
```

Install Node dependencies:
```
cd cubejs-clients/examples/stripe-dashboard
yarn install # or `npm install`
```
### 2. Configure Cube.js API Token
```
cp .env.example .env.local.development
```
You can use test token or change it to your own.

### 3. Run the script
```
yarn start
```
