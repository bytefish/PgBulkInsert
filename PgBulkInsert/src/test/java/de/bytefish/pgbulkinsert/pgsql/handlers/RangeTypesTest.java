// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.geometric.*;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class RangeTypesTest extends TransactionalTestBase {

    private class RangeEntity {
        public Range<ZonedDateTime> timeRange;
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class RangeEntityMapping extends AbstractMapping<RangeEntity> {

        public RangeEntityMapping() {
            super(schema, "time_table");

            mapTsTzRange("col1", (x) -> x.timeRange);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.time_table(\n", schema)
                + "  col1 tstzrange"
                + ");";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }


    @Test
    public void test_SaveTsTzRange_Inclusive_Bounds() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));
        ZonedDateTime upper = ZonedDateTime.of(2020, 3, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));

        entity0.timeRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col1");

            Assert.assertEquals("[\"2020-01-01 01:00:00+01\",\"2020-03-01 01:00:00+01\"]", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_UpperBound_Null() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));
        ZonedDateTime upper = null;

        entity0.timeRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col1");

            Assert.assertEquals("[\"2020-01-01 01:00:00+01\",)", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_LowerBound_Null() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = null;
        ZonedDateTime upper = ZonedDateTime.of(2020, 1, 1, 0, 0, 0 ,0,  ZoneId.of("GMT"));

        entity0.timeRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject) rs.getObject("col1");

            Assert.assertEquals("(,\"2020-01-01 01:00:00+01\"]", v0.getValue());
        }
    }

    @Test
    public void test_SaveTsTzRange_Empty() throws SQLException {

        // This list will be inserted.
        List<RangeEntity> entities = new ArrayList<>();

        // Range to insert:
        RangeEntity entity0 = new RangeEntity();

        ZonedDateTime lower = null;
        ZonedDateTime upper = null;

        entity0.timeRange = new Range<>(lower, upper);

        entities.add(entity0);

        // Construct the Insert:
        PgBulkInsert<RangeEntity> bulkInsert = new PgBulkInsert<>(new RangeEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v0 = (PGobject)rs.getObject("col1");

            Assert.assertEquals("(,)", v0.getValue());
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.time_table", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }



}