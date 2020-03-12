package de.bytefish.pgbulkinsert.rows;


import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NullTerminatingStringTest extends TransactionalTestBase {

    private static final String tableName = "row_writer_test";

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void testWriterThrowsErrorForNullCharacter() throws SQLException {

        // Get the underlying PGConnection:
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);

        // Define the Columns to be inserted:
        String[] columnNames = new String[] {
                "value_text"
        };

        // Create the Table Definition:
        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schema, tableName, columnNames);

        // Create the Writer:
        SimpleRowWriter writer = new SimpleRowWriter(table);

        boolean exceptionHasBeenThrown = false;

        try {
            // ... open it:
            writer.open(pgConnection);

            writer.startRow((row) -> {
                row.setText("value_text", "Hi\0");
            });

            // ... and make sure to close it:
            writer.close();
        } catch(Exception e) {
            exceptionHasBeenThrown = true;
        }


        // Now assert, that we have written 10000 entities:

        Assert.assertEquals(true, exceptionHasBeenThrown);
    }

    @Test
    public void testWriterDoesNotThrowErrorForNullCharacter() throws SQLException {

        // Get the underlying PGConnection:
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);

        // Define the Columns to be inserted:
        String[] columnNames = new String[] {
                "value_text"
        };

        // Create the Table Definition:
        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schema, tableName, columnNames);

        // Create the Writer:
        SimpleRowWriter writer = new SimpleRowWriter(table);

        // ENABLE the Null Character Handler:
        writer.enableNullCharacterHandler();

        boolean exceptionHasBeenThrown = false;

        try {
            // ... open it:
            writer.open(pgConnection);

            writer.startRow((row) -> {
                row.setText("value_text", "Hi\0");
            });

            // ... and make sure to close it:
            writer.close();
        } catch(Exception e) {
            exceptionHasBeenThrown = true;
        }


        // Now assert, that we have written 10000 entities:

        Assert.assertEquals(false, exceptionHasBeenThrown);
    }


    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.%s\n", schema, tableName) +
                "            (\n" +
                "                value_text text\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.%s", schema, tableName));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }
}
