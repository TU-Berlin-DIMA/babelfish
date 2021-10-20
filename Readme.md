# Babelfish: Efficient Execution of Polyglot Queries

This repository provides Babelfish, a prototype for an efficient data processing engine designed for polyglot queries.

## Features:

- Efficient execution of polyglot queries.
- Support for relational operators, e.g., projections, selections, aggregations, and joins
- Support for stateful JavaScript, Python, and Java UDFs
- Efficient text processing
- Data processing over raw data formats, e.g., arrow and csv.

## Polyglot Query Example:

The following example, represents a polyglot version of TCP-H Query 6. It consists of a JavaScript UDF and a Python UDF,
which are embedded into a SQL query.

```JavaScript
// JavaScript map function
function jsMapFunction(r) {
    return l_extendedprice * l_discount;
}
```

```Python
# Python filter function
def pyFilterFunction(r):
    return l_shipdate.after('1994-01-01')
    and l_shipdate.before('1995-01-01')
    and l_discount > 0.05
    and l_discount < 0.07
    and l_quality < 24
```

```SQL
-- SQL Query --
SELECT sum(jsMapFunction(l))
FROM lineitem l
WHERE pyFilterFunction(l)
```

### UDF Examples

Babelfish supports three types of UDFs, selection UDFs, scalar UDFs, and transform UDFs. All UDFs receives an input
record, or a subset of field. Furthermore, UDFs can define additional `open` and `close` to initialize and terminate states.

#### Selection UDF:

A selection UDF receives evaluated a function and returns a boolean value or an integer of one or zero.

```JavaScript
function selectionFunction(record, ctx) {
    return record.l_quantity < 24;
}
```

#### Scalar UDF:

A scalar UDF evaluated a function and returns an arbitrary result record. Babelfish automatically wraps the result to a
BFRecord.

```JavaScript
function scalarFunction(record, ctx) {
    return {'custome_discount_value': record.l_discount * 42}
}
```

#### Transform UDF:

A scalar UDF evaluated a function and returns an arbitrary result record. Babelfish automatically wraps the result to a
BFRecord.

```JavaScript
function transforUDF(record, ctx) {
    for (let i = 0; i < 100; i++) {
        ctx.emit({'custome_discount_value': record.l_discount});
    }
}
```

The complete version will be published as open source as its publication is accepted. The current version represents an
in progress version, such that it may not be reproducible on every system.


#### Text processing UDF
Bablefish supports efficient text processing operations using `PolyglotRopes`.
Currently, Babelfish supports `concat`, `copy`, `equals`, `lowercase`, `uppercase`, `reverse`, `split`, and `substring` operations. 
The following UDF performs `split`, `uppercase`, and `equals`.

```JavaScript
function transforUDF(record, ctx) {
    const words = record.o_comment.split(" ");
    for (let i = 0; i< words.length;i++){
        word = words[i];
        if(word.upper() == "TEST"){
            ctx(words[i]);
        }
    }
}
```

#### UDF that embeds 3rd-party libraries
Bablefish supports efficient execution of UDFs that embed 3rd-party libraries.
In the following example `transforUDF` embeds the  `haversine`
library to calculate the distance between two points.

```Python
from haversine import haversine, Unit
def transforUDF(rec, ctx):   
    start = (rec.start_lat, rec.start_lon)
    end = (rec.end_lat, rec.end_lon)
    distance = haversine(start, end)
    return distance
```

## Status:

This repository provides a prototype of Babelfish. 

#### Dependencies:
Currently, Babelfish relies on GraalVM 20.3 with GraalJs and PyGraal extensions. 
For Graal and both language implementations we provide the following forks:   

https://github.com/TU-Berlin-DIMA/graal   
https://github.com/TU-Berlin-DIMA/graaljs   
https://github.com/TU-Berlin-DIMA/graalpython   

All repositories contain a branch `babelfish` with slight modifications, e.g. extension of Truffles interopt protocol.
All other dependencies are installed via maven. 
Before executing the maven file you have to set yor `jdk.path` in the `pom.xml`.   
Build with: `mvn package -DskipTests`

For detailed build instructions of graal we refer to the following documentation:   
https://github.com/oracle/graal/blob/master/compiler/README.md   
https://github.com/oracle/graal/blob/master/compiler/docs/Debugging.md

#### Startup Instructions:

Babelfish requires specific configuration for the JVM:

```shell
-server
-XX:+UnlockExperimentalVMOptions
-XX:+EnableJVMCI
-XX:+EagerJVMCI
-XX:-UseJVMCINativeLibrary
-XX:-UseJVMCIClassLoader
-d64
-Xbootclasspath/p:compiler/target/classes:benchmark/target/classes
-XX:-UseCompressedOops

```

##### Debug options:

Babelfish relies on Graal thus it supports IGV and XX for debugging the IR, and the final machine code.
`Dgraal.Dump=Truffle:LEVEL` controls the debug level, whereby 2 turned our to be a good granularity for most
usecases.   
`Dgraal.PrintGraph=Network` enables interactive IR visualization in the IGV tool.
`Dgraal.PrintCFG=true` enables debugging the final machine code, with the XX tool.

Example:

```shell
-Dgraal.PrintGraph=Network
-Dgraal.Dump=Truffle:2
```

