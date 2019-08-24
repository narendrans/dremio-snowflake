FROM dremio/dremio-oss
USER root

RUN cd /tmp && \
	wget http://apache.osuosl.org/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.zip && \
	unzip apache-maven-3.6.1-bin.zip && \
	git clone https://github.com/narendrans/dremio-snowflake.git && cd dremio-snowflake && \
	export PATH=$PATH:/tmp/apache-maven-3.6.1/bin && \
	mvn clean install -DskipTests && \
	cp target/dremio-snowflake*.jar /opt/dremio/jars && \
	cd /opt/dremio/jars && wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.0/snowflake-jdbc-3.9.0.jar && \
	chown dremio *snowflake*.jar

USER dremio
