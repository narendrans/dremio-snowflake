package com.dremio.snowflake;


import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectivityTest {

  private static Logger log = Logger.getLogger(ConnectivityTest.class);

  /* Dremio connection settings */
  private final String URL = "http://localhost:9047/";
  private final String dremioUser = "dremio";
  private final String dremioPassword = "dremio123";

  /* Snowflake connection settings */

  String authToken = "_dremio";

  private final String jdbcURL = System.getenv("SNOWFLAKE_JDBC_URL");
  private final String snowflakeUser = System.getenv("SNOWFLAKE_USER");
  private final String snowflakePassword = System.getenv("SNOWFLAKE_PASSWORD");

  @Before
  public void setup() throws IOException, SQLException {

    log.info("Dremio: Get authentication token");
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost("http://localhost:9047/apiv2/login");

    String json = "{\"userName\": \"dremio\",\"password\": \"dremio123\"}";

    httpPost.setEntity(new StringEntity(json));
    httpPost.setHeader("Content-type", "application/json");

    CloseableHttpResponse response = client.execute(httpPost);

    authToken = authToken + new ObjectMapper()
        .readTree(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)).
            findValue("token").asText();

    client.close();

    // Create test table and insert sample data

    log.info("Snowflake: Create test table");
    Properties properties = new Properties();
    properties.put("user", snowflakeUser);
    properties.put("password", snowflakePassword);

    Statement statement = DriverManager.getConnection(jdbcURL, properties).createStatement();

    String sqls[] = FileUtils
        .readFileToString(new File(new File("src/test/resources/DDL.sql").getPath()),
            StandardCharsets.UTF_8).split(";");

    // Snowflake doesn't support executing multiple SQLs in a single call
    statement.executeUpdate(sqls[0]);
    statement.executeUpdate(sqls[1]);

  }

  @Test
  public void connectivityTest() throws IOException {

    log.info("Dremio: Create Snowflake datasource");
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPut httpPut = new HttpPut("http://localhost:9047/apiv2/source/snowflake");

    String json = String.format("{\n"
        + "    \"name\": \"snowflake\",\n"
        + "    \"config\": {\n"
        + "        \"jdbcURL\": \"%s\",\n"
        + "        \"username\": \"%s\",\n"
        + "        \"password\": \"%s\",\n"
        + "        \"fetchSize\": 2000\n"
        + "    },\n"
        + "    \"accelerationRefreshPeriod\": 3600000,\n"
        + "    \"accelerationGracePeriod\": 10800000,\n"
        + "    \"metadataPolicy\": {\n"
        + "        \"deleteUnavailableDatasets\": true,\n"
        + "        \"namesRefreshMillis\": 3600000,\n"
        + "        \"datasetDefinitionRefreshAfterMillis\": 3600000,\n"
        + "        \"datasetDefinitionExpireAfterMillis\": 10800000,\n"
        + "        \"authTTLMillis\": 86400000,\n"
        + "        \"updateMode\": \"PREFETCH_QUERIED\"\n"
        + "    },\n"
        + "    \"accessControlList\": {\n"
        + "        \"userControls\": [],\n"
        + "        \"groupControls\": []\n"
        + "    },\n"
        + "    \"type\": \"SNOWFLAKE\"\n"
        + "}", jdbcURL, snowflakeUser, snowflakePassword);

    httpPut.setEntity(new StringEntity(json));
    httpPut.setHeader("Content-type", "application/json");
    httpPut.setHeader("Authorization", authToken);

    CloseableHttpResponse response = client.execute(httpPut);

    assertEquals(200, response.getStatusLine().getStatusCode());

    client.close();

  }

  @After
  public void cleanUp() throws IOException, SQLException {
    log.info("Removing Snowflake datasource");
    CloseableHttpClient client = HttpClients.createDefault();
    HttpDelete httpDelete = new HttpDelete(
        "http://localhost:9047/apiv2/source/snowflake?version=1");

    httpDelete.setHeader("Content-type", "application/json");
    httpDelete.setHeader("Authorization", authToken);

    client.execute(httpDelete);

    client.close();

    log.info("Snowflake: Remove test table");
    Properties properties = new Properties();
    properties.put("user", snowflakeUser);
    properties.put("password", snowflakePassword);

    Statement statement = DriverManager.getConnection(jdbcURL, properties).createStatement();
    statement.executeUpdate("DROP TABLE \"DEMO_DB\".\"PUBLIC\".\"all_types\"");
  }
}
