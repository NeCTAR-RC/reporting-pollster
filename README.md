# OpenStack Reporting Pollster

This code provides management of the OpenStack reporting service back end
data store, pulling in information from a range of sources and stashing it
in a reporting database that can be accessed via the reporting-api package.

## Deployment

Initial deployment of the application is done in two stages:

* The first stage is the creation of a debian package by the
  [Jenkins](https://github.com/NeCTAR-RC/nectar-ci)
  build server.
* The second stage is the deployment of the package, using
  [puppet](https://github.com/NeCTAR-RC/puppet-reporting) with
  an optional manual step to create the database schema.

Upgrading an existing installation may be as simple as installing the newly
built packages. When a change to the database schema is required the process
is rather more manual, as the database sync script is not currently smart
enough to handle a schema change.

In this case the process should be as follows:

1. Stop the reporting-pollster service
1. Run the database sync script with the --recreate option

```bash
/usr/bin/reporting-db-sync --db-name=<db> \
        --db-user=<user> \
        --db-pass=<password> \
        --schema=/usr/share/doc/python-reporting-pollster/reporting_schema_nectar.sql.gz \
        --recreate
```

1. Restart the reporting-pollster service

Any users of the
[API](https://github.com/NeCTAR-RC/reporting-api) will be affected by schema
changes, though the design of the API makes this relatively easy to deal with.

The application is written to rebuild the data in its tables from scratch
when the database is recreated. It sources this data from the databases
listed in [Database rights](#database-rights)

## Database rights

The application requires read only access to the following OpenStack databases:

* keystone
* nova
* cinder
* glance
* rcshib
* dashboard

## License

Copyright 2015 National Computational Infrastructure

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.