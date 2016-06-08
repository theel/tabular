# tabular

Tabular is a SQL/hive-like DSL and framework for data manipulation.

The goals of Tabular is to 

1. Provide SQL/Hive level abstraction for data manipulation (as opposed to map/reduce operations). This is important as the programming model will be closest to business logic and thus reduce chance of logical bugs, comparing to doing complex map/reduce chains
2. Provide a typed DSL so domain model changed will be detected at compile time (rather than run-time)
3. Be independent of underlying data processing mechanism so the same program is (mostly) transferable between data processing platforms (hadoop, spark, csv etc)

This is work in progress and the only supported mechanism is in-memory List of List (a table with rows).
See the supported DSL https://github.com/theel/tabular/blob/master/src/test/scala/BaseTableTest.scala
