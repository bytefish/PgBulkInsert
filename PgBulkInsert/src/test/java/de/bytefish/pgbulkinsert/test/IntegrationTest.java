package de.bytefish.pgbulkinsert.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.math.BigInteger;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static de.bytefish.pgbulkinsert.PgBulkInsert.*;

public class IntegrationTest {

    private static Connection connection;

    @BeforeAll
    public static void setupDatabase() throws Exception {
        Properties properties = getProperties("db.properties");

        connection = DriverManager.getConnection(
                properties.getProperty("db.url"),
                properties.getProperty("db.user"),
                properties.getProperty("db.password"));

        // Create Test Table handle all the various data types
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS integration_test_data");
            stmt.execute("""
                CREATE TABLE integration_test_data (
                    id int8 PRIMARY KEY,
                    text_val text,
                    numeric_val numeric,
                    numeric_int_val numeric,
                    is_active boolean,
                    created_at timestamp,
                    date_val date,
                    time_val time,
                    timestamptz_val timestamptz,
                    int_range int4range,
                    ts_range tsrange,
                    tags text[]
                )
            """);
        }
    }

    @AfterAll
    public static void teardownDatabase() throws Exception {
        if (connection != null) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS integration_test_data");
            }
            connection.close();
        }
    }

    // Record for the tests
    public record TestEntity(
            long id,
            String textVal,
            BigDecimal numericVal,
            BigInteger numericIntVal,
            boolean isActive,
            LocalDateTime createdAt,
            LocalDate dateVal,
            LocalTime timeVal,
            Instant timestampTzVal,
            PgRange<Integer> intRange,
            PgRange<LocalDateTime> tsRange,
            List<String> tags
    ) {}

    @Test
    public void testBulkInsertSavesDataCorrectly() throws Exception {

        // Build the Test Subject
        PgMapper<TestEntity> mapper = PgMapper.forClass(TestEntity.class)
                .map("id", PostgresTypes.INT8.primitive(TestEntity::id))
                .map("text_val", PostgresTypes.TEXT.removeNullCharacters().from(TestEntity::textVal))
                .map("numeric_val", PostgresTypes.NUMERIC.from(TestEntity::numericVal))
                .map("numeric_int_val", PostgresTypes.NUMERIC_INTEGER.from(TestEntity::numericIntVal))
                .map("is_active", PostgresTypes.BOOLEAN.primitive(TestEntity::isActive))
                .map("created_at", PostgresTypes.TIMESTAMP.localDateTime(TestEntity::createdAt))
                .map("date_val", PostgresTypes.DATE.from(TestEntity::dateVal))
                .map("time_val", PostgresTypes.TIME.from(TestEntity::timeVal))
                .map("timestamptz_val", PostgresTypes.TIMESTAMPTZ.instant(TestEntity::timestampTzVal))
                .map("int_range", PostgresTypes.range(PostgresTypes.INT4).from(TestEntity::intRange))
                .map("ts_range", PostgresTypes.range(PostgresTypes.TIMESTAMP).from(TestEntity::tsRange))
                .map("tags", PostgresTypes.array(PostgresTypes.TEXT).from(TestEntity::tags));

        PgBulkWriter<TestEntity> writer = new PgBulkWriter<>(mapper);

        // Arrange
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS); // Postgres speichert auf Mikrosekunden genau
        LocalDate today = LocalDate.now();
        LocalTime timeNow = LocalTime.now().truncatedTo(ChronoUnit.MICROS);
        Instant instantNow = Instant.now().truncatedTo(ChronoUnit.MICROS);

        List<TestEntity> entities = Arrays.asList(
                new TestEntity(
                        1L, "Normaler Text", new BigDecimal("42.1234"), new BigInteger("98765432101234567890987654321"), true, now,
                        today, timeNow, instantNow,
                        PgRange.closedOpen(1, 100),
                        PgRange.closed(now.minusDays(1), now),
                        List.of("java", "postgres")
                ),
                // Invalid Null to be removed
                new TestEntity(
                        2L, "Fieser \u0000 Text", new BigDecimal("-99.99"), new BigInteger("-12345678909876543210123456789"), false, now.minusDays(1),
                        today.minusDays(1), timeNow.minusHours(1), instantNow.minus(1, ChronoUnit.DAYS),
                        PgRange.emptyRange(),
                        PgRange.atLeast(now),
                        Arrays.asList("test", null, "array")
                )
        );

        // Act
        writer.saveAll(connection, "integration_test_data", entities);

        // Assert
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM integration_test_data ORDER BY id")) {

            // Verify first element
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong("id"));
            assertEquals("Normal Text", rs.getString("text_val"));
            assertEquals(new BigDecimal("42.1234"), rs.getBigDecimal("numeric_val"));
            assertEquals(new BigInteger("98765432101234567890987654321"), rs.getBigDecimal("numeric_int_val").toBigInteger());
            assertTrue(rs.getBoolean("is_active"));
            assertEquals(now, rs.getTimestamp("created_at").toLocalDateTime());

            // Verify temporal types
            assertEquals(today, rs.getObject("date_val", LocalDate.class));
            assertEquals(timeNow, rs.getObject("time_val", LocalTime.class));
            assertEquals(instantNow, rs.getObject("timestamptz_val", OffsetDateTime.class).toInstant());

            // Verify ranges
            assertEquals("[1,100)", rs.getString("int_range"));
            assertTrue(rs.getString("ts_range").contains(now.toLocalDate().toString()));

            java.sql.Array tagsArray1 = rs.getArray("tags");
            String[] tags1 = (String[]) tagsArray1.getArray();
            assertEquals(2, tags1.length);
            assertEquals("java", tags1[0]);

            // Verify Null-Byte Handling
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong("id"));
            assertEquals("Bad  Text", rs.getString("text_val"));
            assertEquals(new BigDecimal("-99.99"), rs.getBigDecimal("numeric_val"));
            assertEquals(new BigInteger("-12345678909876543210123456789"), rs.getBigDecimal("numeric_int_val").toBigInteger());

            // Date Times
            assertEquals(today.minusDays(1), rs.getObject("date_val", LocalDate.class));
            assertEquals(timeNow.minusHours(1), rs.getObject("time_val", LocalTime.class));
            assertEquals(instantNow.minus(1, ChronoUnit.DAYS), rs.getObject("timestamptz_val", OffsetDateTime.class).toInstant());

            // Ranges
            assertEquals("empty", rs.getString("int_range"));
            assertTrue(rs.getString("ts_range").startsWith("["));

            java.sql.Array tagsArray2 = rs.getArray("tags");
            String[] tags2 = (String[]) tagsArray2.getArray();
            assertEquals(3, tags2.length);
            assertEquals("test", tags2[0]);
            assertEquals(null, tags2[1]); // Null im Array korrekt verarbeitet
            assertEquals("array", tags2[2]);

            assertTrue(!rs.next());
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