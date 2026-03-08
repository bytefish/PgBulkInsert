package de.bytefish.pgbulkinsert.test;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegrationTest {

    // Passe diese Verbindungsdaten an deinen laufenden Docker-Container an
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "password";

    private static Connection connection;

    @BeforeAll
    public static void setupDatabase() throws Exception {
        connection = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);

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
            PgBulkInsert.PgRange<Integer> intRange,
            PgBulkInsert.PgRange<LocalDateTime> tsRange,
            List<String> tags
    ) {}

    @Test
    public void testBulkInsertSavesDataCorrectly() throws Exception {

        // Build the Test Subject
        PgBulkInsert.PgMapper<TestEntity> mapper = PgBulkInsert.PgMapper.forClass(TestEntity.class)
                .map("id", PgBulkInsert.PostgresTypes.INT8.primitive(TestEntity::id))
                .map("text_val", PgBulkInsert.PostgresTypes.TEXT.removeNullCharacters().from(TestEntity::textVal))
                .map("numeric_val", PgBulkInsert.PostgresTypes.NUMERIC.from(TestEntity::numericVal))
                .map("numeric_int_val", PgBulkInsert.PostgresTypes.NUMERIC_INTEGER.from(TestEntity::numericIntVal))
                .map("is_active", PgBulkInsert.PostgresTypes.BOOLEAN.primitive(TestEntity::isActive))
                .map("created_at", PgBulkInsert.PostgresTypes.TIMESTAMP.localDateTime(TestEntity::createdAt))
                .map("date_val", PgBulkInsert.PostgresTypes.DATE.from(TestEntity::dateVal))
                .map("time_val", PgBulkInsert.PostgresTypes.TIME.from(TestEntity::timeVal))
                .map("timestamptz_val", PgBulkInsert.PostgresTypes.TIMESTAMPTZ.instant(TestEntity::timestampTzVal))
                .map("int_range", PgBulkInsert.PostgresTypes.range(PgBulkInsert.PostgresTypes.INT4).from(TestEntity::intRange))
                .map("ts_range", PgBulkInsert.PostgresTypes.range(PgBulkInsert.PostgresTypes.TIMESTAMP).from(TestEntity::tsRange))
                .map("tags", PgBulkInsert.PostgresTypes.array(PgBulkInsert.PostgresTypes.TEXT).from(TestEntity::tags));

        PgBulkInsert.PgBulkWriter<TestEntity> writer = new PgBulkInsert.PgBulkWriter<>(mapper);

        // Arrange
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MICROS); // Postgres speichert auf Mikrosekunden genau
        LocalDate today = LocalDate.now();
        LocalTime timeNow = LocalTime.now().truncatedTo(ChronoUnit.MICROS);
        Instant instantNow = Instant.now().truncatedTo(ChronoUnit.MICROS);

        List<TestEntity> entities = Arrays.asList(
                new TestEntity(
                        1L, "Normaler Text", new BigDecimal("42.1234"), new BigInteger("98765432101234567890987654321"), true, now,
                        today, timeNow, instantNow,
                        PgBulkInsert.PgRange.closedOpen(1, 100),
                        PgBulkInsert.PgRange.closed(now.minusDays(1), now),
                        List.of("java", "postgres")
                ),
                // Invalid Null to be removed
                new TestEntity(
                        2L, "Fieser \u0000 Text", new BigDecimal("-99.99"), new BigInteger("-12345678909876543210123456789"), false, now.minusDays(1),
                        today.minusDays(1), timeNow.minusHours(1), instantNow.minus(1, ChronoUnit.DAYS),
                        PgBulkInsert.PgRange.emptyRange(),
                        PgBulkInsert.PgRange.atLeast(now),
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
            assertEquals("Normaler Text", rs.getString("text_val"));
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
            assertEquals("Fieser  Text", rs.getString("text_val")); // \u0000 wurde restlos entfernt!
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
}