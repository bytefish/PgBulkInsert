package de.bytefish.pgbulkinsert.test;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static de.bytefish.pgbulkinsert.PgBulkInsert.*;

public class FirewallRuleIntegrationTest {

    private static Connection connection;

    public record FirewallRule(UUID ruleId, String serverName, List<PgBulkInsert.PgRange<Integer>> openPorts) {}

    @BeforeAll
    static void connect() throws Exception {

        Properties properties = getProperties("db.properties");

        connection = DriverManager.getConnection(
                properties.getProperty("db.url"),
                properties.getProperty("db.user"),
                properties.getProperty("db.password"));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS firewall_rules (
                    rule_id uuid PRIMARY KEY,
                    server_name text,
                    open_ports int4range[]
                )
            """);
        }
    }

    @BeforeEach
    void cleanTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE firewall_rules;");
        }
    }

    @AfterAll
    static void disconnect() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void testInsertInt4RangeArray() throws Exception {
        PgMapper<FirewallRule> mapper = PgMapper.forClass(FirewallRule.class)
                .map("rule_id", PostgresTypes.UUID.from(FirewallRule::ruleId))
                .map("server_name", PostgresTypes.TEXT.from(FirewallRule::serverName))
                .map("open_ports", PostgresTypes.array(PostgresTypes.INT4RANGE).from(FirewallRule::openPorts));

        PgBulkWriter<FirewallRule> writer = new PgBulkWriter<>(mapper);

        FirewallRule webServer = new FirewallRule(
                UUID.randomUUID(),
                "prod-web-01",
                List.of(
                        PgRange.closed(80, 80),         // [80, 80]
                        PgRange.closedOpen(8000, 8081), // [8000, 8081)
                        PgRange.atLeast(50000)          // [50000, infinity)
                )
        );

        // Act
        writer.saveAll(connection, "firewall_rules", List.of(webServer));

        // Assert
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT server_name, open_ports::text as ports_text FROM firewall_rules WHERE server_name = 'prod-web-01'"
            );

            assertTrue(rs.next());
            assertEquals("prod-web-01", rs.getString("server_name"));

            String savedPorts = rs.getString("ports_text");

            System.out.println("Array stored in Postgres: " + savedPorts);

            assertTrue(savedPorts.contains("[8000,8081)"));
            assertTrue(savedPorts.contains("[50000,)"));
        }
    }

    private static Properties getProperties(String filename) {

        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream(filename);

        try {
            props.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not load unittest.properties", e);
        }

        return props;
    }
}