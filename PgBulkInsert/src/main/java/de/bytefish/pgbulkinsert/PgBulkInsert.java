// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.*;

public class PgBulkInsert {


    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value);
    }

    @FunctionalInterface
    public interface ToShortFunction<T> {
        short applyAsShort(T value);
    }

    public record PgRange<T>(T lower, T upper, boolean lowerInclusive, boolean upperInclusive, boolean lowerInfinite,
                             boolean upperInfinite, boolean empty) {
        public static <T> PgRange<T> emptyRange() {
            return new PgRange<>(null, null, false, false, false, false, true);
        }

        public static <T> PgRange<T> closed(T lower, T upper) {
            return new PgRange<>(lower, upper, true, true, false, false, false);
        }

        public static <T> PgRange<T> closedOpen(T lower, T upper) {
            return new PgRange<>(lower, upper, true, false, false, false, false);
        }

        public static <T> PgRange<T> openClosed(T lower, T upper) {
            return new PgRange<>(lower, upper, false, true, false, false, false);
        }

        public static <T> PgRange<T> open(T lower, T upper) {
            return new PgRange<>(lower, upper, false, false, false, false, false);
        }

        public static <T> PgRange<T> atLeast(T lower) {
            return new PgRange<>(lower, null, true, false, false, true, false);
        }

        public static <T> PgRange<T> atMost(T upper) {
            return new PgRange<>(null, upper, false, true, true, false, false);
        }
    }

    public static class PgTimeMath {
        private static final long PG_EPOCH_SECS = 946684800L; // 2000-01-01T00:00:00Z
        private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);

        public static long toPostgresMicros(long epochSeconds, int nanos) {
            return (epochSeconds - PG_EPOCH_SECS) * 1_000_000L + (nanos / 1000);
        }

        public static int toPostgresDays(LocalDate date) {
            return (int) ChronoUnit.DAYS.between(PG_EPOCH_DATE, date);
        }

        public static long toPostgresTimeMicros(LocalTime time) {
            return time.toNanoOfDay() / 1000L;
        }
    }

    public interface BinaryRowWriter {
        void writeNull() throws IOException;

        void writeBoolean(boolean v) throws IOException;

        void writeShort(short v) throws IOException;

        void writeInt(int v) throws IOException;

        void writeLong(long v) throws IOException;

        void writeFloat(float v) throws IOException;

        void writeDouble(double v) throws IOException;

        void writeNumeric(BigDecimal v) throws IOException;

        void writeNumeric(BigInteger v) throws IOException;

        void writeString(String v) throws IOException;

        void writeByteArray(byte[] v) throws IOException;

        void writeDate(int postgresDays) throws IOException;

        void writeTime(long microsSinceMidnight) throws IOException;

        void writePgTimestamp(long postgresMicroseconds) throws IOException;

        void writeUuid(UUID v) throws IOException;

        void writeJsonb(String v) throws IOException;

        void writeHstore(Map<String, String> v) throws IOException;

        <E> void writeArray(Collection<?> elements, PgType<E> baseElementType) throws IOException;

        <E> void writeRange(PgRange<E> range, PgType<E> elementType) throws IOException;
    }


    public static class PgBinaryWriter implements BinaryRowWriter {
        private final DataOutputStream out;

        public PgBinaryWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void writeNull() throws IOException {
            out.writeInt(-1);
        }

        @Override
        public void writeBoolean(boolean v) throws IOException {
            out.writeInt(1);
            out.writeByte(v ? 1 : 0);
        }

        @Override
        public void writeShort(short v) throws IOException {
            out.writeInt(2);
            out.writeShort(v);
        }

        @Override
        public void writeInt(int v) throws IOException {
            out.writeInt(4);
            out.writeInt(v);
        }

        @Override
        public void writeLong(long v) throws IOException {
            out.writeInt(8);
            out.writeLong(v);
        }

        @Override
        public void writeFloat(float v) throws IOException {
            out.writeInt(4);
            out.writeFloat(v);
        }

        @Override
        public void writeDouble(double v) throws IOException {
            out.writeInt(8);
            out.writeDouble(v);
        }

        @Override
        public void writeNumeric(BigDecimal v) throws IOException {
            if (v == null) {
                writeNull();
                return;
            }
            int sign = v.signum() < 0 ? 0x4000 : 0x0000;
            if (v.signum() == 0) {
                out.writeInt(8);
                out.writeShort(0);
                out.writeShort(0);
                out.writeShort(sign);
                out.writeShort(Math.max(0, v.scale()));
                return;
            }

            // Convert to Postgres Numeric (Base 10000)
            String str = v.abs().toPlainString();
            int decPos = str.indexOf('.');
            String whole = decPos < 0 ? str : str.substring(0, decPos);
            String frac = decPos < 0 ? "" : str.substring(decPos + 1);

            while (frac.length() % 4 != 0) frac += "0";

            whole = whole.replaceFirst("^0+(?!$)", "");
            if (whole.isEmpty()) whole = "0";

            int weight;
            if (whole.equals("0")) {
                int firstNonZeroIdx = 0;
                while (firstNonZeroIdx < frac.length() && frac.charAt(firstNonZeroIdx) == '0') firstNonZeroIdx++;
                weight = -1 - (firstNonZeroIdx / 4);
                frac = frac.substring((firstNonZeroIdx / 4) * 4);
                whole = "";
            } else {
                while (whole.length() % 4 != 0) whole = "0" + whole;
                weight = (whole.length() / 4) - 1;
            }

            while (frac.length() >= 4 && frac.endsWith("0000")) frac = frac.substring(0, frac.length() - 4);

            int ndigits = (whole.length() + frac.length()) / 4;

            out.writeInt(8 + ndigits * 2);
            out.writeShort(ndigits);
            out.writeShort(weight);
            out.writeShort(sign);
            out.writeShort(Math.max(0, v.scale()));

            for (int i = 0; i < whole.length(); i += 4) out.writeShort(Integer.parseInt(whole.substring(i, i + 4)));
            for (int i = 0; i < frac.length(); i += 4) out.writeShort(Integer.parseInt(frac.substring(i, i + 4)));
        }

        @Override
        public void writeNumeric(BigInteger v) throws IOException {
            if (v == null) writeNull();
            else writeNumeric(new BigDecimal(v));
        }

        @Override
        public void writeString(String v) throws IOException {
            byte[] b = v.getBytes(StandardCharsets.UTF_8);
            out.writeInt(b.length);
            out.write(b);
        }

        @Override
        public void writeByteArray(byte[] v) throws IOException {
            out.writeInt(v.length);
            out.write(v);
        }

        @Override
        public void writeDate(int postgresDays) throws IOException {
            out.writeInt(4);
            out.writeInt(postgresDays);
        }

        @Override
        public void writeTime(long microsSinceMidnight) throws IOException {
            out.writeInt(8);
            out.writeLong(microsSinceMidnight);
        }

        @Override
        public void writePgTimestamp(long postgresMicroseconds) throws IOException {
            out.writeInt(8);
            out.writeLong(postgresMicroseconds);
        }

        @Override
        public void writeUuid(UUID v) throws IOException {
            out.writeInt(16);
            out.writeLong(v.getMostSignificantBits());
            out.writeLong(v.getLeastSignificantBits());
        }

        @Override
        public void writeJsonb(String v) throws IOException {
            byte[] b = v.getBytes(StandardCharsets.UTF_8);
            out.writeInt(b.length + 1);
            out.writeByte(1); // JSONB Version
            out.write(b);
        }

        @Override
        public void writeHstore(Map<String, String> v) throws IOException {
            int totalByteLength = 4;
            for (Map.Entry<String, String> entry : v.entrySet()) {
                totalByteLength += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length + 4;
                if (entry.getValue() != null)
                    totalByteLength += entry.getValue().getBytes(StandardCharsets.UTF_8).length;
            }
            out.writeInt(totalByteLength);
            out.writeInt(v.size());
            for (Map.Entry<String, String> entry : v.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                if (entry.getValue() == null) {
                    out.writeInt(-1);
                } else {
                    byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(valBytes.length);
                    out.write(valBytes);
                }
            }
        }


        @Override public <E> void writeArray(Collection<?> elements, PgType<E> baseElementType) throws IOException {
            if (elements == null) { writeNull(); return; }

            // Determine dimensions
            List<Integer> dims = new ArrayList<>();
            Object current = elements;
            while (current instanceof Collection) {
                Collection<?> c = (Collection<?>) current;
                dims.add(c.size());
                if (c.isEmpty()) break;
                current = c.iterator().next();
            }

            if (dims.isEmpty()) dims.add(0); // Fallback for empty arrays

            // Flatten the Elements
            List<E> flatElements = new ArrayList<>();
            boolean hasNulls = flattenCollection(elements, flatElements, dims.size());

            // Array Header
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream arrayOut = new DataOutputStream(buffer);

            arrayOut.writeInt(dims.size()); // Number of Dimensions
            arrayOut.writeInt(hasNulls ? 1 : 0); // Has Nulls?
            arrayOut.writeInt(baseElementType.getOid()); // Base OID (INT4...)

            for (int dimSize : dims) {
                arrayOut.writeInt(dimSize);

                arrayOut.writeInt(1); // Lower bound (in Postgres usually 1)
            }

            // Write Elements
            PgBinaryWriter elementWriter = new PgBinaryWriter(arrayOut);
            for (E element : flatElements) {
                if (element == null) elementWriter.writeNull();
                else baseElementType.write(elementWriter, element);
            }

            byte[] arrayBytes = buffer.toByteArray();
            out.writeInt(arrayBytes.length);
            out.write(arrayBytes);
        }

        @SuppressWarnings("unchecked")
        private <E> boolean flattenCollection(Object current, List<E> flat, int depth) {
            boolean hasNulls = false;
            if (depth <= 1) {
                for (Object item : (Collection<?>) current) {
                    if (item == null) hasNulls = true;
                    flat.add((E) item);
                }
            } else {
                for (Object sub : (Collection<?>) current) {
                    if (sub != null) {
                        hasNulls |= flattenCollection(sub, flat, depth - 1);
                    } else {
                        throw new IllegalArgumentException("N-Dimensional Arrays in Postgres aren't allowed to contain Null-Subarrays.");
                    }
                }
            }
            return hasNulls;
        }

        @Override
        public <E> void writeRange(PgRange<E> range, PgType<E> elementType) throws IOException {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream rangeOut = new DataOutputStream(buffer);

            if (range.empty()) {
                rangeOut.writeByte(1); // RANGE_EMPTY flag
            } else {
                byte flags = 0;
                if (range.lowerInclusive()) flags |= 2;
                if (range.upperInclusive()) flags |= 4;
                if (range.lowerInfinite()) flags |= 8;
                if (range.upperInfinite()) flags |= 16;
                rangeOut.writeByte(flags);

                PgBinaryWriter elementWriter = new PgBinaryWriter(rangeOut);

                if (!range.lowerInfinite()) {
                    elementType.write(elementWriter, range.lower());
                }
                if (!range.upperInfinite()) {
                    elementType.write(elementWriter, range.upper());
                }
            }

            byte[] rangeBytes = buffer.toByteArray();

            out.writeInt(rangeBytes.length);
            out.write(rangeBytes);
        }
    }

    @FunctionalInterface
    public interface PgColumn<T> {
        void write(BinaryRowWriter w, T entity) throws IOException;
    }

    public interface PgType<V> {
        void write(BinaryRowWriter w, V value) throws IOException;

        int getOid();

        default <T> PgColumn<T> from(Function<T, V> extractor) {
            return (w, entity) -> {
                V val = extractor.apply(entity);
                if (val == null) w.writeNull();
                else this.write(w, val);
            };
        }
    }

    public interface PgBooleanType extends PgType<Boolean> {
        void writePrimitive(BinaryRowWriter w, boolean v) throws IOException;

        default <T> PgColumn<T> primitive(Predicate<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.test(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Boolean> ext) {
            return from(ext);
        }
    }

    public interface PgShortType extends PgType<Short> {
        void writePrimitive(BinaryRowWriter w, short v) throws IOException;

        default <T> PgColumn<T> primitive(ToShortFunction<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.applyAsShort(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Short> ext) {
            return from(ext);
        }
    }

    public interface PgIntType extends PgType<Integer> {
        void writePrimitive(BinaryRowWriter w, int v) throws IOException;

        default <T> PgColumn<T> primitive(ToIntFunction<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.applyAsInt(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Integer> ext) {
            return from(ext);
        }
    }

    public interface PgLongType extends PgType<Long> {
        void writePrimitive(BinaryRowWriter w, long v) throws IOException;

        default <T> PgColumn<T> primitive(ToLongFunction<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.applyAsLong(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Long> ext) {
            return from(ext);
        }
    }

    public interface PgFloatType extends PgType<Float> {
        void writePrimitive(BinaryRowWriter w, float v) throws IOException;

        default <T> PgColumn<T> primitive(ToFloatFunction<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.applyAsFloat(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Float> ext) {
            return from(ext);
        }
    }

    public interface PgDoubleType extends PgType<Double> {
        void writePrimitive(BinaryRowWriter w, double v) throws IOException;

        default <T> PgColumn<T> primitive(ToDoubleFunction<T> ext) {
            return (w, e) -> this.writePrimitive(w, ext.applyAsDouble(e));
        }

        default <T> PgColumn<T> boxed(Function<T, Double> ext) {
            return from(ext);
        }
    }
    public interface PgStringType extends PgType<String> {

        default PgStringType removeNullCharacters() {
            return replaceNullCharacters("");
        }

        default PgStringType replaceNullCharacters(String replacement) {
            return new PgStringType() {
                @Override public void write(BinaryRowWriter w, String v) throws IOException {
                    PgStringType.this.write(w, v.replace("\0", replacement));
                }
                @Override public int getOid() { return PgStringType.this.getOid(); }
            };
        }
    }

    public interface PgTimestampType extends PgType<LocalDateTime> {
        default <T> PgColumn<T> localDateTime(Function<T, LocalDateTime> ext) {
            return (w, e) -> {
                LocalDateTime ldt = ext.apply(e);
                if (ldt == null) w.writeNull();
                else w.writePgTimestamp(PgTimeMath.toPostgresMicros(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano()));
            };
        }
    }

    public interface PgTimestampTzType extends PgType<Instant> {
        default <T> PgColumn<T> instant(Function<T, Instant> ext) {
            return (w, e) -> {
                Instant i = ext.apply(e);
                if (i == null) w.writeNull();
                else w.writePgTimestamp(PgTimeMath.toPostgresMicros(i.getEpochSecond(), i.getNano()));
            };
        }

        default <T> PgColumn<T> zonedDateTime(Function<T, ZonedDateTime> ext) {
            return (w, e) -> {
                ZonedDateTime zdt = ext.apply(e);
                if (zdt == null) w.writeNull();
                else w.writePgTimestamp(PgTimeMath.toPostgresMicros(zdt.toEpochSecond(), zdt.getNano()));
            };
        }

        default <T> PgColumn<T> offsetDateTime(Function<T, OffsetDateTime> ext) {
            return (w, e) -> {
                OffsetDateTime odt = ext.apply(e);
                if (odt == null) w.writeNull();
                else w.writePgTimestamp(PgTimeMath.toPostgresMicros(odt.toEpochSecond(), odt.getNano()));
            };
        }
    }

    public static final class PostgresTypes {

        public static final PgBooleanType BOOLEAN = new PgBooleanType() {
            @Override
            public int getOid() {
                return 16;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, boolean v) throws IOException {
                w.writeBoolean(v);
            }

            @Override
            public void write(BinaryRowWriter w, Boolean v) throws IOException {
                w.writeBoolean(v);
            }
        };

        public static final PgShortType INT2 = new PgShortType() {
            @Override
            public int getOid() {
                return 21;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, short v) throws IOException {
                w.writeShort(v);
            }

            @Override
            public void write(BinaryRowWriter w, Short v) throws IOException {
                w.writeShort(v);
            }
        };

        public static final PgIntType INT4 = new PgIntType() {
            @Override
            public int getOid() {
                return 23;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, int v) throws IOException {
                w.writeInt(v);
            }

            @Override
            public void write(BinaryRowWriter w, Integer v) throws IOException {
                w.writeInt(v);
            }
        };

        public static final PgLongType INT8 = new PgLongType() {
            @Override
            public int getOid() {
                return 20;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, long v) throws IOException {
                w.writeLong(v);
            }

            @Override
            public void write(BinaryRowWriter w, Long v) throws IOException {
                w.writeLong(v);
            }
        };

        public static final PgFloatType FLOAT4 = new PgFloatType() {
            @Override
            public int getOid() {
                return 700;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, float v) throws IOException {
                w.writeFloat(v);
            }

            @Override
            public void write(BinaryRowWriter w, Float v) throws IOException {
                w.writeFloat(v);
            }
        };

        public static final PgDoubleType FLOAT8 = new PgDoubleType() {
            @Override
            public int getOid() {
                return 701;
            }

            @Override
            public void writePrimitive(BinaryRowWriter w, double v) throws IOException {
                w.writeDouble(v);
            }

            @Override
            public void write(BinaryRowWriter w, Double v) throws IOException {
                w.writeDouble(v);
            }
        };

        public static final PgStringType TEXT = new PgStringType() {
            @Override public int getOid() { return 25; }
            @Override public void write(BinaryRowWriter w, String v) throws IOException { w.writeString(v); }
        };

        public static final PgStringType JSONB = new PgStringType() {
            @Override public int getOid() { return 3802; }
            @Override public void write(BinaryRowWriter w, String v) throws IOException { w.writeJsonb(v); }
        };


        public static final PgType<BigDecimal> NUMERIC = createType(1700, BinaryRowWriter::writeNumeric);
        public static final PgType<BigInteger> NUMERIC_INTEGER = createType(1700, BinaryRowWriter::writeNumeric);
        public static final PgType<byte[]> BYTEA = createType(17, BinaryRowWriter::writeByteArray);
        public static final PgType<UUID> UUID = createType(2950, BinaryRowWriter::writeUuid);
        public static final PgType<Map<String, String>> HSTORE = createType(16384, BinaryRowWriter::writeHstore);

        public static final PgType<LocalDate> DATE = createType(1082, (w, v) -> w.writeDate(PgTimeMath.toPostgresDays(v)));
        public static final PgType<LocalTime> TIME = createType(1083, (w, v) -> w.writeTime(PgTimeMath.toPostgresTimeMicros(v)));

        public static final PgTimestampType TIMESTAMP = new PgTimestampType() {
            @Override
            public int getOid() {
                return 1114;
            }

            @Override
            public void write(BinaryRowWriter w, LocalDateTime v) throws IOException {
                w.writePgTimestamp(PgTimeMath.toPostgresMicros(v.toEpochSecond(ZoneOffset.UTC), v.getNano()));
            }
        };

        public static final PgTimestampTzType TIMESTAMPTZ = new PgTimestampTzType() {
            @Override
            public int getOid() {
                return 1184;
            }

            @Override
            public void write(BinaryRowWriter w, Instant v) throws IOException {
                w.writePgTimestamp(PgTimeMath.toPostgresMicros(v.getEpochSecond(), v.getNano()));
            }
        };

        public static <E> PgType<Collection<E>> array(PgType<E> baseType) {
            return createType(0, (w, v) -> w.writeArray(v, baseType));
        }

        public static <E> PgType<Collection<Collection<E>>> array2D(PgType<E> baseType) {
            return createType(0, (w, v) -> w.writeArray(v, baseType));
        }

        public static <E> PgType<Collection<Collection<Collection<E>>>> array3D(PgType<E> baseType) {
            return createType(0, (w, v) -> w.writeArray(v, baseType));
        }

        public static <E> PgType<PgRange<E>> range(PgType<E> elementType) {
            return createType(0, (w, v) -> w.writeRange(v, elementType));
        }

        private static <V> PgType<V> createType(int oid, WriterFunction<V> writer) {
            return new PgType<V>() {
                @Override
                public void write(BinaryRowWriter w, V v) throws IOException {
                    writer.write(w, v);
                }

                @Override
                public int getOid() {
                    return oid;
                }
            };
        }

        @FunctionalInterface
        interface WriterFunction<V> {
            void write(BinaryRowWriter w, V v) throws IOException;
        }
    }

    public static class PgMapper<T> {
        private final List<String> columnNames = new ArrayList<>();
        private final List<PgColumn<T>> columns = new ArrayList<>();

        public static <T> PgMapper<T> forClass(Class<T> clazz) {
            return new PgMapper<>();
        }

        public PgMapper<T> map(String columnName, PgColumn<T> column) {
            this.columnNames.add(columnName);
            this.columns.add(column);
            return this;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public int getColumnCount() {
            return columns.size();
        }

        public void writeRow(BinaryRowWriter rowWriter, T entity) throws IOException {
            for (PgColumn<T> column : columns) column.write(rowWriter, entity);
        }
    }

    public static class PgBulkWriter<T> {
        private final PgMapper<T> mapper;
        private int bufferSize = 65536;
        private static final byte[] PG_COPY_SIGNATURE = new byte[] { 'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 255, '\r', '\n', '\0' };

        public PgBulkWriter(PgMapper<T> mapper) { this.mapper = mapper; }

        public PgBulkWriter<T> withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public void saveAll(Connection connection, String tableName, Iterable<T> entities) throws SQLException, IOException {
            PGConnection pgConn = connection.unwrap(PGConnection.class);

            String sql = "COPY " + tableName + " (" + String.join(", ", mapper.getColumnNames()) + ") FROM STDIN BINARY";

            try (PGCopyOutputStream copyOut = new PGCopyOutputStream(pgConn, sql);
                 java.io.BufferedOutputStream bufferedOut = new java.io.BufferedOutputStream(copyOut, bufferSize);
                 DataOutputStream dataOut = new DataOutputStream(bufferedOut)) {

                dataOut.write(PG_COPY_SIGNATURE);
                dataOut.writeInt(0); dataOut.writeInt(0);

                PgBinaryWriter binaryWriter = new PgBinaryWriter(dataOut);
                int columnCount = mapper.getColumnCount();

                for (T entity : entities) {
                    dataOut.writeShort(columnCount);
                    mapper.writeRow(binaryWriter, entity);
                }
                dataOut.writeShort(-1);
            }
        }
    }
}