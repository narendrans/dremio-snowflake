package com.dremio.snowflake;


import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SnowflakeTest {

  private static Logger log = Logger.getLogger(SnowflakeTest.class);


  /* Snowflake connection settings */

  private static String authToken = "_dremio";

  private static final String jdbcURL = System.getenv("SNOWFLAKE_JDBC_URL");
  private static final String snowflakeUser = System.getenv("SNOWFLAKE_USER");
  private static final String snowflakePassword = System.getenv("SNOWFLAKE_PASSWORD");

  @BeforeClass
  public static void setup() throws IOException, SQLException {

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

    String[] sqls = FileUtils
        .readFileToString(new File(new File("src/test/resources/DDL.sql").getPath()),
            StandardCharsets.UTF_8).split(";");

    // Snowflake doesn't support executing multiple SQLs in a single call
    statement.executeUpdate(sqls[0]);
    statement.executeUpdate(sqls[1]);

    log.info("Dremio: Create Snowflake datasource");
    CloseableHttpClient createSourceClient = HttpClients.createDefault();
    HttpPut httpPut = new HttpPut("http://localhost:9047/apiv2/source/snowflake");

    String jsonPayload = String.format("{\n"
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

    httpPut.setEntity(new StringEntity(jsonPayload));
    httpPut.setHeader("Content-type", "application/json");
    httpPut.setHeader("Authorization", authToken);


    assertEquals(200, createSourceClient.execute(httpPut).getStatusLine().getStatusCode());


    client.close();

  }

  //TODO: Replace with JDBC calls
  @Test
  public void queryTest() throws IOException {
    log.info("Dremio: SELECT * FROM all_types");
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost("http://localhost:9047/api/v3/sql");

    String queryJson = "{\"sql\": \"SELECT * FROM \\\"snowflake\\\".\\\"DEMO_DB\\\".\\\"PUBLIC\\\".\\\"all_types\\\"\"}";

    httpPost.setEntity(new StringEntity(queryJson));
    httpPost.setHeader("Content-type", "application/json");
    httpPost.setHeader("Authorization", authToken);


    CloseableHttpResponse response = client.execute(httpPost);

    assertEquals(200, response.getStatusLine().getStatusCode());

    String jobId = new ObjectMapper()
        .readTree(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)).
            findValue("id").asText();
    log.info("Submitted Job ID: " + jobId);

    //TODO: Get results from Job and verify returned data
  }

  @AfterClass
  public static void cleanUp() throws IOException, SQLException, InterruptedException {

    log.info("Dremio: Removing Snowflake data source in 5 seconds");
    //TODO: Remove the wait after using JDBC instead of REST
    Thread.sleep(5000);

    CloseableHttpClient client = HttpClients.createDefault();
    HttpDelete httpDelete = new HttpDelete(
        "http://localhost:9047/apiv2/source/snowflake?version=0");

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
