# Open Source Analytics for Slack

## How to Run

To start the Cube.js server with data imported from your Slack export ZIP file, run `npm start path-to-slack-export-file.zip`. Data will be imported into an embedded SQLite database and stored into `db.sqlite`. Remove this file to re-import the data.