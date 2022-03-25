/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.exec.store.jdbc.*;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;

/**
 * Configuration for Snowflake.
 */
@SourceType(value = "SNOWFLAKE", label = "Snowflake", uiConfig = "snowflake-layout.json", externalQuerySupported = true)
public class SnowflakeConf extends AbstractArpConf<SnowflakeConf> {

    private static final String ARP_FILENAME = "arp/implementation/snowflake-arp.yaml";
    private static final ArpDialect ARP_DIALECT =
            AbstractArpConf.loadArpFile(ARP_FILENAME, (SnowflakeDialect::new));
    private static final String DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";
    private static Logger logger = Logger.getLogger(SnowflakeConf.class);

    static class SnowflakeSchemaFetcher extends JdbcSchemaFetcherImpl {

        public SnowflakeSchemaFetcher(JdbcPluginConfig config) {
            super(config);
        }

        protected boolean usePrepareForColumnMetadata() {
            return true;
        }
    }

    static class SnowflakeDialect extends ArpDialect {

        public SnowflakeDialect(ArpYaml yaml) {
            super(yaml);
        }

        @Override
        public JdbcSchemaFetcherImpl newSchemaFetcher(JdbcPluginConfig config) {
            return new SnowflakeSchemaFetcher(config);
        }

        public boolean supportsNestedAggregations() {
            return false;
        }
    }

    /*
       Check Snowflake JDBC connection docs for more details: https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html
     */
    @Tag(1)
    @DisplayMetadata(label = "JDBC URL (Ex: jdbc:snowflake://<account_name>.snowflakecomputing.com/?param1=value&param2=value)")
    public String jdbcURL;

    @Tag(2)
    @DisplayMetadata(label = "Username")
    public String username;

    @Tag(3)
    @Secret
    @DisplayMetadata(label = "Password")
    public String password;

    @Tag(4)
    @DisplayMetadata(label = "Secret resource url")
    public String secretResourceUrl;

    @Tag(5)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 2000;

    //Leave this as JsonIgnore to allow for migration of old data sources
    @Tag(6)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (External Query allows creation of VDS from a Snowflake query. Learn more here: https://docs.dremio.com/data-sources/external-queries.html#enabling-external-queries)")
    @JsonIgnore
    public boolean enableExternalQuery = false;

    @Tag(7)
    @DisplayMetadata(label = "Maximum idle connections")
    @NotMetadataImpacting
    public int maxIdleConns = 8;

    @Tag(8)
    @DisplayMetadata(label = "Connection idle time (s)")
    @NotMetadataImpacting
    public int idleTimeSec = 60;


    @VisibleForTesting
    public String toJdbcConnectionString() {
        checkNotNull(this.jdbcURL, "JDBC URL is required");
        return jdbcURL;
    }

    @Override
    @VisibleForTesting
    public JdbcPluginConfig buildPluginConfig(
            JdbcPluginConfig.Builder configBuilder,
            CredentialsService credentialsService,
            OptionManager optionManager
    ) {
        logger.info("Connecting to Snowflake");
        return configBuilder.withDialect(getDialect())
                .withFetchSize(fetchSize)
                .withDatasourceFactory(() -> newDataSource(credentialsService))
                .clearHiddenSchemas()
                .addHiddenSchema("SYSTEM")
                //.withAllowExternalQuery(enableExternalQuery)
                .build();
    }

    private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {

        PasswordCredentials credsFromCredentialsService = null;
        if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
            try {
                URI secretURI = URI.create(secretResourceUrl);
                credsFromCredentialsService = (PasswordCredentials) credentialsService.getCredentials(secretURI);
            } catch (IOException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }

        return DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), username, (credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : password), null,
                DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, maxIdleConns, idleTimeSec);
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }

    @VisibleForTesting
    public static ArpDialect getDialectSingleton() {
        return ARP_DIALECT;
    }
}