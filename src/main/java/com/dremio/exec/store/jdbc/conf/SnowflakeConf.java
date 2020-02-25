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

import java.util.Properties;

import javax.sql.DataSource;

import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
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
      AbstractArpConf.loadArpFile(ARP_FILENAME, (SnowflakeDialect::new));
  private static final String DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SnowflakeConf.class);

  /*
    The following block is required as Snowflake reports integers as NUMBER(38,0).
   */
  static class SnowflakeSchemaFetcher extends JdbcSchemaFetcher {

    public SnowflakeSchemaFetcher(String name, DataSource dataSource, int timeout, Config config) {
      super(name, dataSource, timeout, config);
    }

    protected boolean usePrepareForColumnMetadata() {
      return true;
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
  }

  /*
     Check Snowflake JDBC connection docs for more details: https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html
   */
  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Account Name (Ex: ab12345.us-east-1)")
  public String accountName;

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
  @DisplayMetadata(label = "Role")
  public String roleName;

  @Tag(6)
  @DisplayMetadata(label = "Warehouse")
  public String warehouseName;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String acctName = checkNotNull(this.accountName, "Missing account name.");
    return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", acctName);
  }

  @Override
  @VisibleForTesting
  public Config toPluginConfig(SabotContext context) {
    logger.info("Connecting to Snowflake");
    return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .build();
  }

  private CloseableDataSource newDataSource() {
    Properties properties = new Properties();

    // Ensure session keep alive is enabled since Dremio likes to keep connections open for the life of the process
    properties.setProperty("CLIENT_SESSION_KEEP_ALIVE", "true");
    properties.setProperty("role", this.roleName);
    properties.setProperty("warehouse", this.warehouseName);

    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
        toJdbcConnectionString(), username, password, properties,
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
