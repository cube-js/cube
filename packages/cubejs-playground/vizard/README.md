# Vizard

Vizard is a web application that allows you to receive an application code example for your framework, visualization library and language using your Cube API params for live preview.

## Setup

Create a `.env.local` file in the root directory of the project and add the following:

```env
# Create the .env.local file in the root of the project and copy the content of this file filling it with your params
VITE_CUBE_API_URL=https://{domain or IP}/cubejs-api/v1
VITE_CUBE_API_TOKEN={YOUR API TOKEN}
VITE_CUBE_QUERY={QUERY IN JSON}
VITE_CUBE_PIVOT_CONFIG={PIVOT CONFIG IN JSON}

```

## Development

```bash
$ yarn dev
```

## Build

```bash
$ yarn build
```

## Preview

```bash
$ yarn preview
```
