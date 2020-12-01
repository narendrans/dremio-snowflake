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

import javax.sql.DataSource;

import com.dremio.exec.store.jdbc.*;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

/**
 * Configuration for Snowflake.
 */
@SourceType(value = "SNOWFLAKE", label = "Snowflake")
public class SnowflakeConf extends AbstractArpConf<SnowflakeConf> {

  private static final String ARP_FILENAME = "arp/implementation/snowflake-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";
  private static Logger logger = Logger.getLogger(SnowflakeConf.class);

  /*
    The following block is required as Snowflake reports integers as NUMBER(38,0).
   */
  static class SnowflakeSchemaFetcher implements JdbcSchemaFetcher {

    public SnowflakeSchemaFetcher(String name, DataSource dataSource, int timeout, Config config) {
      super();
    }

    protected boolean usePrepareForColumnMetadata() {
      return true;
    }

    @Override
    public JdbcFetcherProto.GetStateResponse getState(JdbcFetcherProto.GetStateRequest getStateRequest) {
      return null;
    }

    @Override
    public JdbcFetcherProto.CatalogOrSchemaExistsResponse catalogOrSchemaExists(JdbcFetcherProto.CatalogOrSchemaExistsRequest catalogOrSchemaExistsRequest) {
      return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }
  }

  static class SnowflakeDialect extends ArpDialect {

    public SnowflakeDialect(ArpYaml yaml) {
      super(yaml);
    }

    public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout,
        JdbcStoragePlugin.Config config) {
      return new SnowflakeSchemaFetcher(name, dataSource, timeout, config);
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
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 2000;

  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Grant External Query access (External Query allows creation of VDS from a Snowflake query. Learn more here: https://docs.dremio.com/data-sources/external-queries.html#enabling-external-queries)")
  public boolean enableExternalQuery = false;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    checkNotNull(this.jdbcURL, "JDBC URL is required");
    return jdbcURL;
  }

  @Override
  @VisibleForTesting
  public Config toPluginConfig(CredentialsService credentialsService, OptionManager optionManager) {
    logger.info("Connecting to Snowflake");
    return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .withAllowExternalQuery(enableExternalQuery)
        .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
        toJdbcConnectionString(), username, password, null,
        DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
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