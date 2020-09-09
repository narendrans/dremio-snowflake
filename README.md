# Dremio Snowflake Connector

<img src="https://www.dremio.com/img/dremio-website.png" width="60"> <img src="https://www.snowflake.com/wp-content/themes/snowflake/img/snowflake-logo-blue@2x.png" width="200">


[![Codacy Badge](https://api.codacy.com/project/badge/Grade/ecc264fe94074379afc080f2e1549630)](https://app.codacy.com/app/narendrans/dremio-snowflake?utm_source=github.com&utm_medium=referral&utm_content=narendrans/dremio-snowflake&utm_campaign=Badge_Grade_Dashboard)
[![Build Status](https://travis-ci.org/narendrans/dremio-snowflake.svg?branch=master)](https://travis-ci.org/narendrans/dremio-snowflake)
![Last Commit](https://img.shields.io/github/last-commit/narendrans/dremio-snowflake)
[![Docker build](https://img.shields.io/docker/cloud/build/narendrans/dremio-snowflake.svg)](https://hub.docker.com/r/narendrans/dremio-snowflake/builds)

![Latest Release](https://img.shields.io/github/v/release/narendrans/dremio-snowflake)
![License](https://img.shields.io/badge/license-Apache%202-blue)
![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macos%20%7C%20windows-blue)
[![Chat](https://img.shields.io/gitter/room/Dremio-Snowflake-Connector/community)](https://gitter.im/Dremio-Snowflake-Connector/community)

<!--ts-->
   * [Overview](#overview)
      * [Use Cases](#use-cases)
      * [Features](#features)
      * [Demo](#demo)
   * [Downloading a Release](#downloading-a-release)
   * [Usage](#usage)
   * [Development](#development)
      * [Building and Installation](#building-and-installation)
      * [Building a Docker image](#building-a-docker-image)
      * [Debugging](#debugging)
   * [Contribution](#contribution)
      * [Submitting an issue](#submitting-an-issue)
      * [Pull Requests](#pull-requests)
   * [Troubleshooting](#troubleshooting)
<!--te-->

Overview
-----------

This is a community based Snowflake Dremio connector made using the ARP framework. Check [Dremio Hub](https://github.com/dremio-hub) for more examples and [ARP Docs](https://github.com/dremio-hub/dremio-sqllite-connector#arp-file-format) for documentation. 

What is Dremio?
-----------

Dremio delivers lightning fast query speed and a self-service semantic layer operating directly against your data lake storage and other sources. No moving data to proprietary data warehouses or creating cubes, aggregation tables and BI extracts. Just flexibility and control for Data Architects, and self-service for Data Consumers.

Use Cases
-----------

* [Join data](https://www.dremio.com/tutorials/combining-data-from-multiple-datasets/) from Snowflake with other sources (On prem/Cloud)
* Interactive SQL performance with [Data Reflections](https://www.dremio.com/tutorials/getting-started-with-data-reflections/)
* Offload Snowflake tables using [CTAS](https://www.dremio.com/tutorials/high-performance-parallel-exports/) to your cheap data lake storage - HDFS, S3, ADLS
  * Or use [COPY INTO](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html) to export data from Snowflake into S3/ADLS and query them directly using Dremio or [create external reflections](https://docs.dremio.com/acceleration/creating-reflections.html#external-reflections) on top of them.
* [Curate Datasets](https://www.dremio.com/tutorials/data-curation-with-dremio/) easily through the self-service platform

Features
-----------

* Complete datatype support
* Pushdown of over 50+ functions
* Verified push downs of all TPCH queries


Demo
-----------

![Snowflake demo](snowflake.gif)

Downloading a Release
-----------

* To download a release, [click here](https://github.com/narendrans/dremio-snowflake/releases)
* To test it using Docker, run the following command:

`docker run -p 9047:9047 -p 31010:31010 narendrans/dremio-snowflake`

Usage
-----------

### Creating a new Snowflake Source

### Required Parameters

* JDBC URL
    * Ex: `jdbc:snowflake://<account_name>.snowflakecomputing.com/?param1=value&param2=value`. [More details](https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html).
* Username, Password
    * The username and password with which you want to connect to Snowflake. Password is not needed if you want to use a PEM file. In that case you can use a JDBC string like below (The pem must exist on all the nodes)
    `jdbc:snowflake://account.us-east-1.snowflakecomputing.com?warehouse=compute_wh&private_key_file=/Users/naren/Desktop/rsa_key.pem`

## Development

Building and Installation
-----------

0. Change the pom's dremimo.version to suit your Dremio's version. `version.dremio>4.7.3-202008270723550726-918276ee</version.dremio>`
1. In root directory with the pom.xml file run `mvn clean install -DskipTests`. If you want to run the tests, add the JDBC jar to your local maven repo along with environment variables that are required. Check the basic test example for more details.
2. Take the resulting .jar file in the target folder and put it in the <DREMIO_HOME>\jars folder in Dremio
3. Download the Snowflake JDBC driver from (https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.8.6 and click on the JAR link) and put in in the <DREMIO_HOME>\jars\3rdparty folder
4. Restart Dremio

Building a Docker image
-------
Note: You can pull the pre-built docker images: https://hub.docker.com/r/narendrans/dremio-snowflake

Dockerfile:

```
FROM dremio/dremio-oss:4.2.2
USER root

WORKDIR /tmp

RUN wget http://apache.osuosl.org/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.zip && \
	unzip apache-maven-3.6.1-bin.zip && \
	git clone https://github.com/narendrans/dremio-snowflake.git && cd dremio-snowflake && \
	export PATH=$PATH:/tmp/apache-maven-3.6.1/bin && \
	mvn clean install -DskipTests && \
	cp target/dremio-snowflake*.jar /opt/dremio/jars && \
	cd /opt/dremio/jars && wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.1/snowflake-jdbc-3.9.1.jar && \
	chown dremio *snowflake*.jar && rm -rf ~/.m2 && rm -rf /tmp/*

WORKDIR /opt/dremio
USER dremio
```

Build:

`docker build . -t dremio-snowflake`

Run:

`docker run -p 9047:9047 -p 31010:31010 dremio-snowflake`

Debugging
-----------
To debug pushdowns for queries set the following line in `logback.xml`

```
  <logger name="com.dremio.exec.store.jdbc">
    <level value="${dremio.log.level:-trace}"/>
  </logger>
 ```
  
You can then notice lines like below in server.log file after which you can revist the YAML file to add pushdowns based on [Snowflake SQL Reference](https://docs.snowflake.net/manuals/sql-reference-commands.html):

```diff
- 2019-07-11 18:56:24,001 [22d879a7-ce3d-f2ca-f380-005a88865700/0:foreman-planning] DEBUG c.d.e.store.jdbc.dialect.arp.ArpYaml - Operator / not supported. Aborting pushdown.
```

You can also take a look at the planning tab/visualized plan of the profile to determine if everything is pushed down or not.

Contribution
------------

### Submitting an issue

* Go to the issue submission page: https://github.com/narendrans/dremio-snowflake/issues/new/choose. Please select an appropriate category and provide as much details as you can.

### Pull Requests

PRs are welcome. When submitting a PR make sure of the following:

* Try to follow Google's Java style coding when modifying/creating Java related content.
* Use a YAML linter to check the syntactic correctness of YAML file
* Make sure the build passes
* Run basic queries at least to ensure things are working properly

Troubleshooting
------------

### Snowflake unable to create the cache directory

If you see the following trace in dremio:

```
Caused by: java.lang.RuntimeException: Failed to locate or create the cache directory: /home/dremio/.cache/snowflake
        at net.snowflake.client.core.FileCacheManager.build(FileCacheManager.java:159) ~[snowflake-jdbc-3.8.7.jar:3.8.7]
        at net.snowflake.client.core.SFTrustManager.<clinit>(SFTrustManager.java:197) ~[snowflake-jdbc-3.8.7.jar:3.8.7]
        ... 21 common frames omitted
 ```
You should then set the File cache environment variables documented [here](https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#file-caches)

```
export SF_TEMPORARY_CREDENTIAL_CACHE_DIR=<path>
export SF_OCSP_RESPONSE_CACHE_DIR=<path>
```

To set them as JAVA properties, add them to the [conf/dremio-env file](https://docs.dremio.com/advanced-administration/dremio-env.html)

`DREMIO_JAVA_SERVER_EXTRA_OPTS='-Dnet.snowflake.jdbc.temporaryCredentialCacheDir=/tmp -Dnet.snowflake.jdbc.ocspResponseCacheDir=/tmp'`
