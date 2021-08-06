---
title: Enable SSL Connections to the Database
permalink: /recipes/enable-ssl-connections-to-database
category: Examples & Tutorials
subCategory: Security
menuOrder: 1
---

Cube.js supports SSL-encrypted connections for:

- [ClickHouse][ref-config-db-clickhouse]
- [MongoDB][ref-config-db-mongodb]
- [MS SQL][ref-config-db-mssql]
- [MySQL][ref-config-db-mysql]
- [Postgres][ref-config-db-postgres]

To enable it, set the `CUBEJS_DB_SSL` environment variable to `true`. Cube.js
can also be configured to use custom connection settings. For example, to use a
custom certificate authority and certificates, you could do the following:

```dotenv
CUBEJS_DB_SSL_CA=/path/to/ssl/ca.pem
CUBEJS_DB_SSL_CERT=/path/to/ssl/cert.pem
CUBEJS_DB_SSL_KEY=/path/to/ssl/key.pem
```

You can also set the above environment variables to the contents of the PEM
files; for example:

```dotenv
CUBEJS_DB_SSL_CA="-----BEGIN CERTIFICATE-----
MIIDDjCCAfYCCQCN/HhSZ3ofTDANBgkqhkiG9w0BAQsFADBJMQswCQYDVQQGEwJV
SzEMMAoGA1UECgwDSUJNMQ0wCwYDVQQLDARBSU9TMR0wGwYDVQQDDBRhaW9zLW9y
Y2gtZGV2LWVudi1DQTAeFw0yMTAyMTUyMzIyMTZaFw0yMzEyMDYyMzIyMTZaMEkx
CzAJBgNVBAYTAlVLMQwwCgYDVQQKDANJQk0xDTALBgNVBAsMBEFJT1MxHTAbBgNV
BAMMFGFpb3Mtb3JjaC1kZXYtZW52LUNBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAyhYY9+4TduTsNRh/6MaRtE59j8HkAkoQYvNYZN7D1j1oV6yhzitn
oN4bD+HiQWe4J3mwAaJOAAJRCkIVyUXxwZUCPxGN/KVha/pcB8hN6LHfI6vInixp
U9kHNYWWBn428nMeMqts7yqly/HwG1/qO+j4178c8lZNS7Uwh76y+lAEaIkeBipq
i4WuCOiChFc/sIV7g4DcLKKbqzDWtRDjbsg7JRfsALO5gM360GrNYkhV4C5lm8Eh
ozNuaPhS65zO93PMj/3UTyuctXKa7WpaHJHoKZRXAuOwSamvqvFgIQ0SSnW+qcud
fL3GAPJn7d065gh7JvgcT86v7WWBiUNs0QIDAQABMA0GCSqGSIb3DQEBCwUAA4IB
AQCzw00d8e0e5AYZtzIk9hjczta7JHy2/cwTMv0opzBk6C26G6YZww+9brHW2w5U
mY/HKBnGnMadjMWOZmm9Vu0B0kalYY0lJdE8alO1aiv5B9Ms/XIt7FzzGtfv9gYJ
cw5/nzGBBMJNICC1kVLnzzlllLferhCIrczDyPcu16o1Flc7q1p8AbwQpC+A2I/L
8nWlFeHZ+watLtQ1lF3qDzzCumPHrJqAGmlp0265owCM8Q5zv8AL5DStIZvtexrI
JqbwLdbA8smyOFRwCckOWcWjnrEDjO2e3NLWINbB7Z4ZRviZSEH5UZlDLVu+ahGV
KmZIuh7+XpXzJ1MN0SBZXgXH
-----END CERTIFICATE-----"
```

For a complete list of SSL-related environment variables, consult the [Database
Connections section of the Environment Variables Reference][ref-env-var].

[ref-config-db-clickhouse]: /config/databases/clickhouse
[ref-config-db-mongodb]: /config/databases/mongodb
[ref-config-db-mssql]: /config/databases/mssql
[ref-config-db-mysql]: /config/databases/mysql
[ref-config-db-postgres]: /config/databases/postgres
[ref-env-var]: /reference/environment-variables#database-connection
