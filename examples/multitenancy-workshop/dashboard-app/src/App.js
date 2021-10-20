import {
  BrowserRouter as Router,
  Switch,
  Route,
  Link,
  useParams
} from "react-router-dom";
import cubejs from "@cubejs-client/core";
import './App.css';
import { useEffect, useState } from "react";

const merchants = [
  { id: 1, jwt: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjUwMDAwMDAwMDAsIm1lcmNoYW50SWQiOjF9.X3MoDOpLChCZjdKv34EjlC3Y9jdSn2WsPSywj9A_6V8' },
  { id: 2, jwt: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjUwMDAwMDAwMDAsIm1lcmNoYW50SWQiOjJ9.V1X2A2jIpk7no-C0TmXC6j8VYz9o_C4_eGF0cYAiWZM' },
  { id: 3, jwt: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjUwMDAwMDAwMDAsIm1lcmNoYW50SWQiOjN9._t-xHNKtLBkogugdA0hTKBXla_1eIEcLGasJO3-gtDs' },
];

function App() {
  return (
    <Router>
      <Switch>
          <Route exact path="/">
            <div className="App">
              <h1>Homepage</h1>
              <ul>
                {merchants.map((x, i) => (
                  <li key={i}>
                    <Link to={`/dashboard/${x.jwt}`}>{x.id}</Link>
                  </li>
                ))}
              </ul>
            </div>
          </Route>
          <Route path="/dashboard/:jwt">
            <MerchantDashboard />
          </Route>
        </Switch>
    </Router>
  );
}

function MerchantDashboard() {
  let { jwt } = useParams();
  let [ data, setData ] = useState([]);

  const cubejsApi = cubejs(
    jwt,
    { apiUrl: "https://awesome-ecom.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1" }
  );

  const resultSet = cubejsApi.load({
    measures: ["Orders.count"],
    dimensions: ["ProductCategories.name"]
  });

  useEffect(() => {
    if (resultSet) {
      resultSet.then(data => setData(data.tablePivot()));
    }
  }, [ resultSet ]);

  return (
    <div className="App">
      <h1>Merchant Dashboard</h1>
      <p><Link to="/">Back</Link></p>
      <p>JWT: {jwt}</p>
      <pre>Data: {JSON.stringify(data, null, 2)}</pre>
    </div>
  );
}



export default App;
