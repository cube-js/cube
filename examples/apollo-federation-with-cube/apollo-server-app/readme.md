# Deploy steps
Make sure to install Netlify CLI.
Needs to be deployed manually with CLI because the build container on Netlify fails due to node-gyp.

1. `npm i`
2. `netlify build`
3. `netlify deploy --dir=dist --functions=functions`
4. `netlify deploy --prod`

# Run locally
Run the local express server for local development.

```
npm i
npm start
```
