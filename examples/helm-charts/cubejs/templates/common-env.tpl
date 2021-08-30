{{- define "cubejs.common-env" -}}
- name: PORT
  value: {{ .Values.global.apiPort | quote }}
{{- if .Values.global.debug }}
- name: DEBUG_LOG
  value: {{ .Values.global.debug | quote }}
{{- end }}
{{- if .Values.global.devMode }}
- name: CUBEJS_DEV_MODE
  value: {{ .Values.global.devMode | quote }}
{{- end }}
{{- if .Values.global.logLevel }}
- name: CUBEJS_LOG_LEVEL
  value: {{ .Values.global.logLevel | quote }}
{{- end }}
{{- if .Values.global.app }}
- name: CUBEJS_APP
  value: {{ .Values.global.app | quote }}
{{- end }}
{{- if .Values.global.cacheAndQueueDriver }}
- name: CUBEJS_CACHE_AND_QUEUE_DRIVER
  value: {{ .Values.global.cacheAndQueueDriver | quote }}
{{- end }}
{{- if .Values.global.rollupOnly }}
- name: CUBEJS_ROLLUP_ONLY
  value: {{ .Values.global.rollupOnly | quote }}
{{- end }}
{{- if .Values.global.scheduledRefreshTimezones }}
- name: CUBEJS_SCHEDULED_REFRESH_TIMEZONES
  value: {{ .Values.global.scheduledRefreshTimezones | quote }}
{{- end }}
{{- if .Values.global.preAggregationsSchema }}
- name: CUBEJS_PRE_AGGREGATIONS_SCHEMA
  value: {{ .Values.global.preAggregationsSchema | quote }}
{{- end }}
{{- if .Values.global.webSockets }}
- name: CUBEJS_WEB_SOCKETS
  value: {{ .Values.global.webSockets | quote }}
{{- end }}
- name: CUBEJS_TELEMETRY
  value: {{ .Values.global.telemetry | quote }}
{{- if .Values.global.apiSecret }}
- name: CUBEJS_API_SECRET
  value: {{ .Values.global.apiSecret | quote }}
{{- else if .Values.global.apiSecretFromSecret }}
- name: CUBEJS_API_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.global.apiSecretFromSecret.name | required "global.apiSecretFromSecret.name is required" }}
      key: {{ .Values.global.apiSecretFromSecret.key | required "global.apiSecretFromSecret.key is required" }}
{{- end }}
{{- if .Values.global.schemaPath }}
- name: CUBEJS_SCHEMA_PATH
  value: {{ .Values.global.schemaPath | quote }}
{{- end }}
{{- if .Values.global.topicName }}
- name: CUBEJS_TOPIC_NAME
  value: {{ .Values.global.topicName | quote }}
{{- end }}
{{- if .Values.redis.url }}
- name: CUBEJS_REDIS_URL
  value: {{ .Values.redis.url | quote }}
{{- end }}
{{- if .Values.redis.password }}
- name: CUBEJS_REDIS_PASSWORD
  value: {{ .Values.redis.password | quote }}
{{- else if .Values.redis.passwordFromSecret }}
- name: CUBEJS_REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.passwordFromSecret.name | required "redis.passwordFromSecret.name is required" }}
      key: {{ .Values.redis.passwordFromSecret.key | required "redis.passwordFromSecret.key is required" }}
{{- end }}
{{- if .Values.redis.tls }}
- name: CUBEJS_REDIS_TLS
  value: {{ .Values.redis.tls | quote }}
{{- end }}
{{- if .Values.redis.poolMin }}
- name: CUBEJS_REDIS_POOL_MIN
  value: {{ .Values.redis.poolMin | quote }}
{{- end }}
{{- if .Values.redis.poolMax }}
- name: CUBEJS_REDIS_POOL_MAX
  value: {{ .Values.redis.poolMax | quote }}
{{- end }}
{{- if .Values.redis.useIoRedis }}
- name: CUBEJS_REDIS_USE_IOREDIS
  value: {{ .Values.redis.useIoRedis | quote }}
{{- end }}
{{- if .Values.jwt.url }}
- name: CUBEJS_JWK_URL
  value: {{ .Values.jwt.url | quote }}
{{- end }}
{{- if .Values.jwt.key }}
- name: CUBEJS_JWT_KEY
  value: {{ .Values.jwt.key | quote }}
{{- else if .Values.jwt.keyFromSecret }}
- name: CUBEJS_JWT_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.jwt.keyFromSecret.name | required "jwt.keyFromSecret.name is required" }}
      key: {{ .Values.jwt.keyFromSecret.key | required "jwt.keyFromSecret.key is required" }}
{{- end }}
{{- if .Values.jwt.audience }}
- name: CUBEJS_JWT_AUDIENCE
  value: {{ .Values.jwt.audience | quote }}
{{- end }}
{{- if .Values.jwt.issuer }}
- name: CUBEJS_JWT_ISSUER
  value: {{ .Values.jwt.issuer | quote }}
{{- end }}
{{- if .Values.jwt.subject }}
- name: CUBEJS_JWT_SUBJECT
  value: {{ .Values.jwt.subject | quote }}
{{- end }}
{{- if .Values.jwt.algs }}
- name: CUBEJS_JWT_ALGS
  value: {{ .Values.jwt.algs | quote }}
{{- end }}
{{- if .Values.jwt.claimsNamespace }}
- name: CUBEJS_JWT_CLAIMS_NAMESPACE
  value: {{ .Values.jwt.claimsNamespace | quote }}
{{- end }}
- name: CUBEJS_DB_TYPE
  value: {{ .Values.database.type | quote | required "database.type is required" }}
{{- if .Values.database.url }}
- name: CUBEJS_DB_URL
  value: {{ .Values.database.url | quote }}
{{- end }}
{{- if .Values.database.host }}
- name: CUBEJS_DB_HOST
  value: {{ .Values.database.host | quote }}
{{- end }}
{{- if .Values.database.port }}
- name: CUBEJS_DB_PORT
  value: {{ .Values.database.port | quote }}
{{- end }}
{{- if .Values.database.schema }}
- name: CUBEJS_DB_SCHEMA
  value: {{ .Values.database.schema | quote }}
{{- end }}
{{- if .Values.database.name }}
- name: CUBEJS_DB_NAME
  value: {{ .Values.database.name | quote }}
{{- end }}
{{- if .Values.database.user }}
- name: CUBEJS_DB_USER
  value: {{ .Values.database.user | quote }}
{{- end }}
{{- if .Values.database.pass }}
- name: CUBEJS_DB_PASS
  value: {{ .Values.database.pass | quote }}
{{- else if .Values.database.passFromSecret }}
- name: CUBEJS_DB_PASS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.passFromSecret.name | required "database.passFromSecret.name is required" }}
      key: {{ .Values.database.passFromSecret.key | required "database.passFromSecret.key is required" }}
{{- end }}
{{- if .Values.database.domain }}
- name: CUBEJS_DB_DOMAIN
  value: {{ .Values.database.domain | quote }}
{{- end }}
{{- if .Values.database.socketPath }}
- name: CUBEJS_DB_SOCKET_PATH
  value: {{ .Values.database.socketPath | quote }}
{{- end }}
{{- if .Values.database.catalog }}
- name: CUBEJS_DB_CATALOG
  value: {{ .Values.database.catalog | quote }}
{{- end }}
{{- if .Values.database.maxPool }}
- name: CUBEJS_DB_MAX_POOL
  value: {{ .Values.database.maxPool | quote }}
{{- end }}
{{- if .Values.database.aws.key }}
- name: CUBEJS_AWS_KEY
  value: {{ .Values.database.aws.key | quote }}
{{- else if .Values.database.aws.keyFromSecret }}
- name: CUBEJS_AWS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.aws.keyFromSecret.name | required "database.key.keyFromSecret.name is required" }}
      key: {{ .Values.database.aws.keyFromSecret.key | required "database.key.keyFromSecret.key is required" }}
{{- end }}
{{- if .Values.database.aws.region }}
- name: CUBEJS_AWS_REGION
  value: {{ .Values.database.aws.region | quote }}
{{- end }}
{{- if .Values.database.aws.outputLocation }}
- name: CUBEJS_AWS_OUTPUT_LOCATION
  value: {{ .Values.database.aws.outputLocation | quote }}
{{- end }}
{{- if .Values.database.aws.secret }}
- name: CUBEJS_AWS_SECRET
  value: {{ .Values.database.aws.secret | quote }}
{{- else if .Values.database.aws.secretFromSecret }}
- name: CUBEJS_AWS_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.aws.secretFromSecret.name | required "database.key.secretFromSecret.name is required" }}
      key: {{ .Values.database.aws.secretFromSecret.key | required "database.key.secretFromSecret.key is required" }}
{{- end }}
{{- if .Values.database.aws.athenaWorkgroup }}
- name: CUBEJS_AWS_ATHENA_WORKGROUP
  value: {{ .Values.database.aws.athenaWorkgroup | quote }}
{{- end }}
{{- if .Values.database.bigquery.projectId }}
- name: CUBEJS_DB_BQ_PROJECT_ID
  value: {{ .Values.database.bigquery.projectId | quote }}
{{- end }}
{{- if .Values.database.bigquery.location }}
- name: CUBEJS_DB_BQ_LOCATION
  value: {{ .Values.database.bigquery.location | quote }}
{{- end }}
{{- if .Values.database.bigquery.credentials }}
- name: CUBEJS_DB_BQ_CREDENTIALS
  value: {{ .Values.database.bigquery.credentials | quote }}
{{- else if .Values.database.bigquery.credentialsFromSecret }}
- name: CUBEJS_DB_BQ_CREDENTIALS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.database.bigquery.credentialsFromSecret.name | required "database.bigquery.credentialsFromSecret.name is required" }}
      key: {{ .Values.database.bigquery.credentialsFromSecret.key | required "database.bigquery.credentialsFromSecret.key is required" }}
{{- end }}
{{- if .Values.exportBucket.name }}
- name: CUBEJS_DB_EXPORT_BUCKET
  value: {{ .Values.exportBucket.name | quote }}
{{- end }}
{{- if .Values.exportBucket.type }}
- name: CUBEJS_DB_EXPORT_BUCKET_TYPE
  value: {{ .Values.exportBucket.type | quote }}
{{- end }}
{{- if .Values.exportBucket.gcsCredentials }}
- name: CUBEJS_DB_EXPORT_GCS_CREDENTIALS
  value: {{ .Values.exportBucket.gcsCredentials | quote }}
{{- else if .Values.exportBucket.gcsCredentialsFromSecret }}
- name: CUBEJS_DB_EXPORT_GCS_CREDENTIALS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.exportBucket.gcsCredentialsFromSecret.name | required "exportBucket.gcsCredentialsFromSecret.name is required" }}
      key: {{ .Values.exportBucket.gcsCredentialsFromSecret.key | required "exportBucket.gcsCredentialsFromSecret.key is required" }}
{{- end }}
{{- if .Values.database.hive.cdhVersion }}
- name: CUBEJS_DB_HIVE_CDH_VER
  value: {{ .Values.database.hive.cdhVersion | quote }}
{{- end }}
{{- if .Values.database.hive.thriftVersion }}
- name: CUBEJS_DB_HIVE_THRIFT_VER
  value: {{ .Values.database.hive.thriftVersion | quote }}
{{- end }}
{{- if .Values.database.hive.type }}
- name: CUBEJS_DB_HIVE_TYPE
  value: {{ .Values.database.hive.type | quote }}
{{- end }}
{{- if .Values.database.hive.version }}
- name: CUBEJS_DB_HIVE_VER
  value: {{ .Values.database.hive.version | quote }}
{{- end }}
{{- if .Values.database.jdbc.driver }}
- name: CUBEJS_JDBC_DRIVER
  value: {{ .Values.database.jdbc.driver | quote }}
{{- end }}
{{- if .Values.database.jdbc.url }}
- name: CUBEJS_JDBC_URL
  value: {{ .Values.database.jdbc.url | quote }}
{{- end }}
{{- if .Values.database.snowFlake.account }}
- name: CUBEJS_DB_SNOWFLAKE_ACCOUNT
  value: {{ .Values.database.snowFlake.account | quote }}
{{- end }}
{{- if .Values.database.snowFlake.region }}
- name: CUBEJS_DB_SNOWFLAKE_REGION
  value: {{ .Values.database.snowFlake.region | quote }}
{{- end }}
{{- if .Values.database.snowFlake.role }}
- name: CUBEJS_DB_SNOWFLAKE_ROLE
  value: {{ .Values.database.snowFlake.urolerl | quote }}
{{- end }}
{{- if .Values.database.snowFlake.warehouse }}
- name: CUBEJS_DB_SNOWFLAKE_WAREHOUSE
  value: {{ .Values.database.snowFlake.warehouse | quote }}
{{- end }}
{{- if .Values.database.snowFlake.clientSessionKeepAlive }}
- name: CUBEJS_DB_SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE
  value: {{ .Values.database.snowFlake.clientSessionKeepAlive | quote }}
{{- end }}
{{- if .Values.database.snowFlake.authenticator }}
- name: CUBEJS_DB_SNOWFLAKE_AUTHENTICATOR
  value: {{ .Values.database.snowFlake.authenticator | quote }}
{{- end }}
{{- if .Values.database.snowFlake.privateKeyPath }}
- name: CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH
  value: {{ .Values.database.snowFlake.privateKeyPath | quote }}
{{- end }}
{{- if .Values.database.snowFlake.urprivateKeyPassl }}
- name: CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PASS
  value: {{ .Values.database.snowFlake.privateKeyPass | quote }}
{{- end }}
{{- if .Values.database.databricks.url }}
- name: CUBEJS_DB_DATABRICKS_URL
  value: {{ .Values.database.databricks.url | quote }}
{{- end }}
{{- if .Values.database.ssl.enabled }}
- name: CUBEJS_DB_SSL
  value: "true"
{{- if .Value.database.ssl.rejectUnAuthorized }}
- name: CUBEJS_DB_SSL_REJECT_UNAUTHORIZED
  value: {{ .Value.database.ssl.rejectUnAuthorized | quote }}
{{- end }}
{{- if .Value.database.ssl.ca }}
- name: CUBEJS_DB_SSL_CA
  value: {{ .Value.database.ssl.ca | quote }}
{{- end }}
{{- if .Value.database.ssl.cert }}
- name: CUBEJS_DB_SSL_CERT
  value: {{ .Value.database.ssl.cert | quote }}
{{- end }}
{{- if .Value.database.ssl.key }}
- name: CUBEJS_DB_SSL_KEY
  value: {{ .Value.database.ssl.key | quote }}
{{- end }}
{{- if .Value.database.ssl.ciphers }}
- name: CUBEJS_DB_SSL_CIPHERS
  value: {{ .Value.database.ssl.ciphers | quote }}
{{- end }}
{{- if .Value.database.ssl.serverName }}
- name: CUBEJS_DB_SSL_SERVERNAME
  value: {{ .Value.database.ssl.serverName | quote }}
{{- end }}
{{- if .Value.database.ssl.passPhrase }}
- name: CUBEJS_DB_SSL_PASSPHRASE
  value: {{ .Value.database.ssl.passPhrase | quote }}
{{- end }}
{{- end }}
{{- if .Values.cubestore.host }}
- name: CUBEJS_CUBESTORE_HOST
  value: {{ .Values.cubestore.host | quote }}
{{- end }}
{{- if .Values.cubestore.port }}
- name: CUBEJS_CUBESTORE_PORT
  value: {{ .Values.cubestore.port | quote }}
{{- end }}
{{- if .Values.externalDatabase.type }}
- name: CUBEJS_EXT_DB_TYPE
  value: {{ .Values.externalDatabase.type | quote }}
{{- end }}
{{- if .Values.externalDatabase.host }}
- name: CUBEJS_EXT_DB_HOST
  value: {{ .Values.externalDatabase.host | quote }}
{{- end }}
{{- if .Values.externalDatabase.name }}
- name: CUBEJS_EXT_DB_NAME
  value: {{ .Values.externalDatabase.name | quote }}
{{- end }}
{{- if .Values.externalDatabase.pass }}
- name: CUBEJS_EXT_DB_PASS
  value: {{ .Values.externalDatabase.pass | quote }}
{{- else if .Values.externalDatabase.passFromSecret }}
- name: CUBEJS_EXT_DB_PASS
  valueFrom:
    secretKeyRef:
      name: {{ .Values.externalDatabase.passFromSecret.name | required "externalDatabase.passFromSecret.name is required" }}
      key: {{ .Values.externalDatabase.passFromSecret.key | required "externalDatabase.passFromSecret.key is required" }}
{{- end }}
{{- if .Values.externalDatabase.user }}
- name: CUBEJS_EXT_DB_USER
  value: {{ .Values.externalDatabase.user | quote }}
{{- end }}
{{- if .Values.externalDatabase.port }}
- name: CUBEJS_EXT_DB_PORT
  value: {{ .Values.externalDatabase.port | quote }}
{{- end }}
{{- end }}
