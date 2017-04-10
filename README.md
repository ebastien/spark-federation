A Spark skeleton for a pushed-down filtered aggregate
=====================================================

This is a proof of concept showing how to complement
the standard Spark SQL data sources API to push down
more complex expressions to the data sources.

We capture a filtered aggregation from the Spark SQL
logical plan and generate a simple RDD physical scan out of it.
The captured expressions would then be used by our customized
RDD to return the appropriate iterators from
each executor.

The next step of the demonstration would be to serve
those RDD partitions from some kind of web service,
mapping the captured expressions to a proper set of
parameters or even a SQL dialect.
