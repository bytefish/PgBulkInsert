# PgBulkInsert #

[MIT License]: https://opensource.org/licenses/MIT
[COPY command]: http://www.postgresql.org/docs/current/static/sql-copy.html
[PgBulkInsert]: https://github.com/bytefish/PgBulkInsert
[Npgsql]: https://github.com/npgsql/npgsql

PgBulkInsert is a Java library for Bulk Inserts to PostgreSQL using the Binary COPY Protocol. 

It provides a wrapper around the PostgreSQL [COPY command]:

> The [COPY command] is a PostgreSQL specific feature, which allows efficient bulk import or export of 
> data to and from a table. This is a much faster way of getting data in and out of a table than using 
> INSERT and SELECT.

This project wouldn't be possible without the great [Npgsql] library, which has a beautiful implementation of the Postgres protocol.

## Setup ##

[PgBulkInsert] is available in the Central Maven Repository. 

You can add the following dependencies to your pom.xml to include [PgBulkInsert] in your project.

```xml
<dependency>
    <groupId>de.bytefish</groupId>
    <artifactId>pgbulkinsert</artifactId>
    <version>9.0.0</version>
</dependency>
```

## PgBulkInsert 9: Modern, Functional API ##

PgBulkInsert 9.0.0 comes with a new API, that focuses on a functional API surface leveraging modern Java 
features (Lambdas, Method References, and Records) and reduces memory allocation-overhead.

## Quick Start ##

The new API separates the What (Structure and Mapping) from the How (Execution and Configuration).

### 1. Define your Data Model ###

The library works with Java Records, POJOs, or any other data carrier.

```java
public record UserSession(
    UUID id,
    long visits,         // Primitive
    String userAgent,    // Nullable String
    Instant createdAt,   // Precise Timestamp
    double[] latLon,     // Array
    PgPoint location     // Geometric Point
) {}
```

### 2. Define your Mapping ###

The `PgMapper` is the heart of the library. It is stateless and thread-safe.

```java
PgMapper<UserSession> mapper = PgMapper.forClass(UserSession.class)
    .map("id", PostgresTypes.UUID.from(UserSession::id))
    
    // ZERO-ALLOCATION: Direct access to primitives via ToLongFunction
    .map("visits", PostgresTypes.INT8.primitive(UserSession::visits))
    
    // SAFE STRINGS: Strips invalid \u0000 characters to prevent COPY errors
    .map("user_agent", PostgresTypes.TEXT.removeNullCharacters().from(UserSession::userAgent))
    
    // TYPE-SAFE TIME: Validated at compile-time to prevent timezone issues
    .map("created_at", PostgresTypes.TIMESTAMPTZ.instant(UserSession::createdAt))
    
    // GEOMETRY: Native support for Postgres geometric types
    .map("location", PostgresTypes.POINT.from(UserSession::location));
```

### 3. Configure and Execute ###

The `PgBulkWriter` handles execution details like buffer sizes and stream management.

```java
PgBulkWriter<UserSession> writer = new PgBulkWriter<>(mapper)
    .withBufferSize(256 * 1024); // 256 KB Buffer

try (Connection conn = dataSource.getConnection()) {
    writer.saveAll(conn, "public.user_sessions", sessionList);
}
```

## Streaming and Lazy Evaluation ##

One of the key strengths of the `saveAll` method is that it accepts an `Iterable<T>`. This means you are never 
forced to load your entire dataset into memory.

If you are working with a Java Stream (e.g., from a file, a reactive source, or another database), you 
can pass it directly using a method reference:

```java
Stream<UserSession> massiveStream = getMassiveStreamFromSource();

try (Connection conn = dataSource.getConnection()) {
    // Uses the stream's iterator to pull data lazily
    writer.saveAll(conn, "public.user_sessions", massiveStream::iterator);
}
```

This approach ensures that records are transformed and written to the PostgreSQL wire format on-the-fly, 
keeping your application's memory usage constant regardless of the total number of rows.

## Mastering the Fluent API ##

The API is designed around a so called `PostgresType`. This class serves as your single 
entry point for all PostgreSQL data types.

### The Power of PostgresTypes ###

Instead of a generic `map()` method that tries to guess your intent, the API uses a "Type-First" 
approach. When you type `PostgresTypes.INT4.`, your IDE will offer specific choices:

* `.primitive(ToIntFunction<T>)`: High-performance path. No objects created on the heap.
* `.boxed(Function<T, Integer>)`: Use this for nullable database columns or if your POJO uses Integer.
* `.from(Function<T, Integer>)`: Standard object mapping.

### Eliminating Timezone Confusion ###

One of the most common bugs is the confusion between `timestamp` and `timestamptz`.

The API solves this:

* `PostgresTypes.TIMESTAMP` only allows `.localDateTime()`.
* `PostgresTypes.TIMESTAMPTZ` allows `.instant()`, `.zonedDateTime()`, or `.offsetDateTime()`.

## Advanced Type Mapping ##

### N-Dimensional Arrays (Matrices & Tensors) ###

```java
// Mapping a 2D Matrix (Collection of Collections)
.map("data_matrix", PostgresTypes.array2D(PostgresTypes.INT4).from(MyEntity::getMatrix))

// Mapping a 3D Tensor
.map("data_tensor", PostgresTypes.array3D(PostgresTypes.FLOAT8).from(MyEntity::getTensor))
```

### Ranges ###

```java
// Mapping an integer range
.map("age_limit", PostgresTypes.INT4RANGE.from(MyEntity::getAgeRange))

// Mapping a timestamp range
.map("validity", PostgresTypes.TSRANGE.from(MyEntity::getValidityPeriod))
```

### Geometric Types ###

Native support for all PostgreSQL geometric types using dedicated helper records:

* `POINT`: `PostgresTypes.POINT`
* `CIRCLE`: `PostgresTypes.CIRCLE`
* `POLYGON`: `PostgresTypes.POLYGON`
* `PATH` / `LSEG` / `BOX` / `LINE`

```java
.map("area", PostgresTypes.POLYGON.from(MyEntity::getBoundaries))
```

## Supported PostgreSQL Types ##

* [Numeric Types](http://www.postgresql.org/docs/current/static/datatype-numeric.html)
    * smallint
    * integer
    * bigint
    * real
    * double precision
	* numeric
* [Date/Time Types](http://www.postgresql.org/docs/current/static/datatype-datetime.html)
    * timestamp
    * timestamptz
    * date
    * time
    * interval
* [Character Types](http://www.postgresql.org/docs/current/static/datatype-character.html)
    * text
* [JSON Types](https://www.postgresql.org/docs/current/static/datatype-json.html)
    * jsonb
* [Boolean Type](http://www.postgresql.org/docs/current/static/datatype-boolean.html)
    * boolean
* [Binary Data Types](http://www.postgresql.org/docs/current/static/datatype-binary.html)
    * bytea
* [Network Address Types](http://www.postgresql.org/docs/current/static/datatype-net-types.html)
    * inet (IPv4, IPv6)
    * macaddr
* [UUID Type](http://www.postgresql.org/docs/current/static/datatype-uuid.html)
    * uuid
* [Array Type](https://www.postgresql.org/docs/current/static/arrays.html)
    * One-Dimensional Arrays
* [Range Type](https://www.postgresql.org/docs/current/rangetypes.html)
    * int4range
    * int8range
    * numrange
    * tsrange
    * tstzrange
    * daterange
* [hstore](https://www.postgresql.org/docs/current/static/hstore.html)
    * hstore
* [Geometric Types](https://www.postgresql.org/docs/current/static/datatype-geometric.html)
    * point
    * line
    * lseg
    * box
    * path
    * polygon
    * circle
