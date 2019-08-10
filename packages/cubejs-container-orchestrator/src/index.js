const { json } = require("body-parser");
const morgan = require("morgan");
const { app, start } = require("./server");

app.disable("x-powered-by");

app.use(json({ limit: "25mb" }));
app.use(morgan(process.env.NODE_ENV === "development" ? "dev" : "common"));

if (process.env.NODE_ENV === "test") {
  app.get("/", (req, res) => {
    res.send("Hello World!");
  });
}

start();
