## What's in this package

This repo contains examples of applications built using RecordService client APIs.

- `RSCat`: output tabular data for any data set readable by RecordService

- `SumQueryBenchmark`: This is an example of running a simple sum over a column,
  pushing the scan to RecordService.

- `Terasort`: terasort ported to RecordService. See README in package for more
  details. This also demonstrates how to implement a custom InputFormat using
  the RecordService APIs.

- `MapredColorCount`/`MapreduceAgeCount`/`MapReduceColorCount`: These are the examples
  ported from Apache Avro and demonstrate the steps required to port an existing
  Avro-based MapReduce job to use RecordService.

- `RecordCount`/`Wordcount`: More simple MapReduce applications that demonstrate some
  of the other InputFormats that are included in the client library.

- `com.cloudera.recordservice.examples.avro`: Unmodified from the [Apache Avro](https://avro.apache.org/) examples.
  We've included these to simplify sample data generation.

## How to enforce Sentry permissions with MapReduce

With RecordService, MapReduce users can now enforce restrictions on data with **views**. Below is a
example shows how to do this. We also have examples showing how to do this in Spark. Please
check the [Spark example](../examples-spark/README.md#how-to-enforce-sentry-permissions-with-spark) for details.
Also, make sure the Sentry Service is started. In the QuickStart VM, this can simply be done by:

```bash
sudo service sentry-store restart
```

### Granting Permissions to Views

In this section we'll create a demo group and role. We will also create a view on a selected
subset of columns to an existing table, and grant access of that view to the demo role.

First, create a group and add the current user to that group:

```bash
sudo groupadd demogroup
sudo usermod -a -G demogroup $USER
```

Then, start up Impala or Hive (we use Impala here, but one can also use Beeline in Hive to do the same thing) and 
create a view on an existing table. For demonstration, here we use
`tpch.nation` (the table is also available in our QuickStart VM) as example.

This is how the schema for `tpch.nation` looks like:

| column_name | column_type |
|-------------|-------------|
| n_nationkey | smallint    |
| n_name      | string      |
| n_regionkey | smallint    |
| n_comment   | string      |

Suppose we want some users with a particular role to only be able to read the
 `n_nationkey` and `n_name` columns, we can do the following:

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > CREATE ROLE demorole;
[quickstart.cloudera:21000] > GRANT ROLE demorole to GROUP demogroup;
[quickstart.cloudera:21000] > USE tpch;
[quickstart.cloudera:21000] > CREATE VIEW nation_names AS SELECT n_nationkey, n_name FROM tpch.nation;
[quickstart.cloudera:21000] > GRANT SELECT ON TABLE tpch.nation_names TO ROLE demorole;
```

This first creates a role called `demorole`, then add the role
to the group `demogroup` we created above. It then creates a view on the `n_nationkey` and `n_name` columns of
the `tpch.nation` table, and grant the **select** privilege to the `demorole`.

### Running MR Jobs

With the above settings, now try to launch a MR job for [RecordCount](src/main/java/com/cloudera/recordservice/examples/mapreduce/RecordCount.java)
on `tpch.nation`:

```bash
hadoop jar /path/to/recordservice-examples-0.1.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT * FROM tpch.nation" \
  "/tmp/recordcount_output"
```

It will quickly fail with this exception:

```
TRecordServiceException(code:INVALID_REQUEST, message:Could not plan request.,
detail:AuthorizationException: User 'cloudera' does not have privileges to execute 'SELECT' on:
tpch.nation)
```

Then try to access the `tpch.nation_names` view:
 
```bash
hadoop jar /path/to/recordservice-examples-0.1.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT * FROM tpch.nation_names" \
  "/tmp/recordcount_output"
```

It will succeed with the correct result.
