#!/bin/bash

PUBLIC_URL="PUBLIC_URL=${UFFIZZI_URL}/dashboard-url/"
REACT_APP="REACT_APP_API_URL=${UFFIZZI_URL}"
CUBEJS_API="CUBEJS_API_URL=${UFFIZZI_URL}"

sed -i "s|PUBLIC_URL.*|$PUBLIC_URL|" "./dashboard-app/.env.development"
sed -i "s|REACT_APP_API_URL.*|$REACT_APP|" "./dashboard-app/.env.development"
sed -i "s|CUBEJS_API_URL.*|$CUBEJS_API|" "./dashboard-app/.env.development"



