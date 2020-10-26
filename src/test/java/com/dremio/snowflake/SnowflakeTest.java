package com.dremio.snowflake;


import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;
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

  private static final String snowflakeJdbcURL = System.getenv("SNOWFLAKE_JDBC_URL");
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

    Statement statement = DriverManager.getConnection(snowflakeJdbcURL, properties)
        .createStatement();

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
        + "}", snowflakeJdbcURL, snowflakeUser, snowflakePassword);

    httpPut.setEntity(new StringEntity(jsonPayload));
    httpPut.setHeader("Content-type", "application/json");
    httpPut.setHeader("Authorization", authToken);

    assertEquals(200, createSourceClient.execute(httpPut).getStatusLine().getStatusCode());

    client.close();

  }


  @Test
  public void queryTest() throws IOException, SQLException {
    log.info("Dremio: SELECT * FROM all_types");

    TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));

    // Get resultset from Dremio
    Connection dremioConnection = DriverManager
        .getConnection("jdbc:dremio:direct=localhost;user=dremio;password=dremio123");

    Statement dremioStatement = dremioConnection.createStatement();

    ResultSet dremioRs = dremioStatement
        .executeQuery("SELECT * FROM snowflake.\"DEMO_DB\".\"PUBLIC\".all_types");
    dremioRs.next();

    // Get resultset from Snowflake
    Properties properties = new Properties();
    properties.put("user", snowflakeUser);
    properties.put("password", snowflakePassword);
    Connection snowflakeConnection = DriverManager
        .getConnection(snowflakeJdbcURL, properties);

    Statement snowflakeStatement = snowflakeConnection.createStatement();

    ResultSet snowflakeRs = snowflakeStatement
        .executeQuery("SELECT * FROM \"DEMO_DB\".\"PUBLIC\".\"all_types\"");
    snowflakeRs.next();


    // Compare

    assertEquals(dremioRs.getBigDecimal("A"), snowflakeRs.getBigDecimal("A"));
    assertEquals(dremioRs.getInt("B"), snowflakeRs.getInt("B"));
    assertEquals(dremioRs.getInt("C"), snowflakeRs.getInt("C"));
    assertEquals(dremioRs.getInt("D"), snowflakeRs.getInt("D"));
    assertEquals(dremioRs.getInt("E"), snowflakeRs.getInt("E"));
    assertEquals(dremioRs.getInt("F"), snowflakeRs.getInt("F"));

    assertEquals(dremioRs.getDouble("G"), snowflakeRs.getDouble("G"),0.1);
    assertEquals(dremioRs.getDouble("H"), snowflakeRs.getDouble("H"),0.1);
    assertEquals(dremioRs.getDouble("I"), snowflakeRs.getDouble("I"),0.1);
    assertEquals(dremioRs.getDouble("J"), snowflakeRs.getDouble("J"),0.1);
    assertEquals(dremioRs.getDouble("K"), snowflakeRs.getDouble("K"),0.1);
    assertEquals(dremioRs.getDouble("L"), snowflakeRs.getDouble("L"),0.1);

    assertEquals(dremioRs.getString("M"), snowflakeRs.getString("M"));
    assertEquals(dremioRs.getString("N"), snowflakeRs.getString("N"));
    assertEquals(dremioRs.getString("O"), snowflakeRs.getString("O"));
    assertEquals(dremioRs.getString("P"), snowflakeRs.getString("P"));

    assertEquals(new String(dremioRs.getBytes("Q")), new String(snowflakeRs.getBytes("Q")));
    assertEquals(new String(dremioRs.getBytes("R")), new String(snowflakeRs.getBytes("R")));

    assertEquals(dremioRs.getTimestamp("S"), snowflakeRs.getTimestamp("S"));
    //assertEquals(dremioRs.getTime("T"), snowflakeRs.getTime("T"));
    assertEquals(dremioRs.getTimestamp("U"), snowflakeRs.getTimestamp("U"));


    dremioStatement.close();
    snowflakeStatement.close();

  }

  @AfterClass
  public static void cleanUp() throws IOException, SQLException, InterruptedException {

    log.info("Dremio: Removing Snowflake data source in 5 seconds");

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

    Statement statement = DriverManager.getConnection(snowflakeJdbcURL, properties)
        .createStatement();
    statement.executeUpdate("DROP TABLE \"DEMO_DB\".\"PUBLIC\".\"all_types\"");
  }
}
