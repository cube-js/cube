---
order: 3
title: "Step 1. Authentication with JWTs"
---

As we already know, the essence of authentication is making sure that our application is accessed by verified users, and not by anyone else. How do we achieve that?

We can ask users to pass a piece of information from the web application to the API. If we can verify that this piece of information is valid and it passes our checks, we'll allow that user to access our app. Such a piece of information is usually called a *token*.

[JSON Web Tokens](https://jwt.io) are an open, industry-standard method for representing such pieces of information with additional information (so-called *claims*). Cube.js, just like many other apps, uses JWTs to authenticate requests to the API.

Now, we're going to update the API to authenticate the requests and make sure the web application sends the correct JWTs.

**First, let's update the Cube.js configuration.** In the `.env` file, you can find the following options:

```ini
CUBEJS_DEV_MODE=true
CUBEJS_API_SECRET=SECRET
```

The first option controls if Cube.js should run in the [development mode](https://cube.dev/docs/configuration/overview?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#development-mode). In that mode, all authentication checks are disabled. The second option sets the key used to cryptographically sign JWTs. It means that, if we keep this key secret, only we'll be able to generate JWTs for our users.

Let's update these options (and add a new one, described in [docs](https://cube.dev/docs/reference/environment-variables?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#general)):

```
CUBEJS_DEV_MODE=false
CUBEJS_API_SECRET=NEW_SECRET
CUBEJS_CACHE_AND_QUEUE_DRIVER=memory
```

Instead of `NEW_SECRET`, you should generate and use a new pseudo-random string. One way to do that might be to use an [online generator](https://www.uuidgenerator.net). Another option is to run this simple Python command in your console and copy-paste the result:

```sh
python -c 'import sys,uuid; sys.stdout.write(uuid.uuid4().hex)'
```

After that, save the updated `.env` file, stop Cube.js (by pressing `CTRL+C`), and run Cube.js again with `npm run dev`. You'll see a message without mentioning the Development Mode in the console and Developer Playground will no longer be present at [localhost:4000](https://localhost:4000).

**Second, let's check that the web application is broken. 🙀** It should be because we've just changed the security key and didn't bother to provide a correct JWT. Here's what we'll see if we repeat the `curl` command in the console:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/0f1kc9s31vorfq2zahf8.png)

Looks legit. But what's that "Authorization header", exactly? It's an HTTP header called `Authorization` which is used by Cube.js to [authenticate](https://cube.dev/docs/rest-api?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#prerequisites-authentication) the requests. We didn't pass anything like that via the `curl` command, hence the result. And here's what we'll see if we reload our web application:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/eung09ctqch0ckliwxny.png)

Indeed, it's broken as well. Great, we're going to fix it.

**Finally, let's generate a new JWT and fix the web application.** You can use lots of [libraries](https://jwt.io) to work with JWTs, but Cube.js provides a convenient way to generate tokens in the command line. Run the following command, substituting `NEW_SECRET` with your key generated on the first step:

```sh
npx cubejs-cli token --secret="NEW_SECRET" --payload="role=admin"
```

You'll see something like this:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/2o1eqezymjulb4dud62p.png)

The output provides the following insights:
* We've created a new JWT: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJ1Ijp7fSwiaWF0IjoxNjE1MTY1MDYwLCJleHAiOjE2MTc3NTcwNjB9.IWpKrqD71dkLxyJRuiii6YEfxGYU_xxXtL-l2zU_VPY` (your token should be different because your key is different).
* It will expire in 30 days (we could control the expiration period with the `--expiry` option but 30 days are enough for our purposes).
* It contains additional information (`role=admin`) which we'll use later for authorization.

We can go to [jwt.io](https://jwt.io), copy-paste our token, and check if it really contains the info above. Just paste your JWT in the giant text field on the left. You'll see something like this:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/4knwzos149cgzpyfect9.png)

Did you miss those "30 days"? They are encoded in the `exp` property as a timestamp, and you surely can [convert](https://www.unixtimestamp.com) the value back to a human-readable date. You can also check the signature by pasting your key into the "Verify Signature" text input and re-pasting your JWT.

Now we're ready to fix the web application. Open the `dashboard-app/src/App.js` file. After a few imports, you'll see the lines like this:

```js
const API_URL = "http://localhost:4000";
const CUBEJS_TOKEN = "SOME_TOKEN";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});
```

These lines configure the Cube.js [client library](https://cube.dev/docs/frontend-introduction?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) to look for the API at `localhost:4000` and pass a particular token. Change `SOME_TOKEN` to the JWT you've just generated and verified, then stop the web application (by pressing `CTRL+C`), and run it again with `npm start`. We'll see that the web application works again and passes the JWT that we've just added to the API with the `Authorization` header:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/r2rpntn8xd9elql16jf3.png)

To double-check, we can run the same query with the same header in the console:

```sh
curl http://localhost:4000/cubejs-api/v1/load \
  -H 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4iLCJpYXQiOjE2MTUxNjUwNjAsImV4cCI6MTYxNzc1NzA2MH0.BNC8xlkB8vmuT0T6s1a5cZ3jXwhcHrAVNod8Th_Wzqw' \
  -G -s --data-urlencode 'query={"measures": ["Orders.count"], "dimensions": ["Orders.status"]}' \
  | jq '.data'
```

Make sure to check that if you remove the header or change just a single symbol of the token, the API returns an error, and never then result.

‼️ **We were able to add authentication and secure the API with JSON Web Tokens.** Now the API returns the result only if a valid JWT is passed. To generate such a JWT, one should know the key which is currently stored in the `.env` file.

Now, as we're becalmed, it's time to proceed to the next step and add authorization 🤿