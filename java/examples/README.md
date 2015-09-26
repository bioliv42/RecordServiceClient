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

With RecordService, MapReduce users can now enforce restrictions on data using Sentry
privileges and provide fine grain (row and column-level) authorization using Hive Metastore
views. Here we show two simple examples on how to do this: one using **SQL query**, and
another using **path request**. We also have examples showing how to do this in Spark. Please
check the [Spark example](../examples-spark/README.md#how-to-enforce-sentry-permissions-with-spark) for details.

Before jumping into the examples, please make sure the Sentry Service is started.
In the QuickStart VM, this can simply be done by:

```bash
sudo service sentry-store restart
```

### Reading data through SQL query

If the data is a registered table in Hive MetaStore, one can access it through
SQL queries and enforce fine-grained restrictions with **views**. The following shows
how to do this.

#### Granting permission to view

In this section we'll first create a demo group and role, then create a view on a selected
subset of columns to an existing table, and grant access of that view to the demo role.

First, create a group and add the current user to that group:

```bash
sudo groupadd demogroup
sudo usermod -a -G demogroup $USER
```

Then, start up Impala or Hive (we use Impala here, but one can also use Beeline in Hive
to do the same thing) and create a view on an existing table. For demonstration, here
we use `tpch.nation` (the table is also available in our QuickStart VM) as example.

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
to the group `demogroup` we created above. It then creates a view on the `n_nationkey`
and `n_name` columns of the `tpch.nation` table, and grant the **select** privilege to
the `demorole`.

#### Running MR Job

Now, if we want to count the number of records in the `tpch.nation` with the above settings,
we can launch a MR job for [RecordCount](src/main/java/com/cloudera/recordservice/examples/mapreduce/RecordCount.java) on the table:

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

### Reading data through path request

Data can also be ready directly via **path request**, rather than specifying a projection
on a table. This is useful for reading data that is not backed by a Hive Metastore tables
as well as a way to expedite migration of existing applications which directly read files
to RecordService.

Path request requires a few different settings SQL query request. To demonstrate, we use
the path ``/test-warehouse/tpch.nation`` for the `tpch.nation` table as example.

#### Granting permission to path

First, make sure that ``sentry.hive.server`` is properly set in the ``sentry-site.xml``.

Similar to the SQL query example, we first need to grant privilege (please note that at
the moment it requires **ALL** privilege) to the chosen path. In addition, **for path
request the user who's running the ``recordserviced`` process needs to have the privilege
to create databases**. This is because internally RecordService needs to create a
temporary database and table for the path.

Assume the RecordService Planner is running as a user in the 'recordservice' group:

```bash
sudo -u impala impala-shell
[quickstart.cloudera:21000] > GRANT ALL ON URI 'hdfs:/test-warehouse/tpch.nation' TO ROLE demorole;
[quickstart.cloudera:21000] > CREATE ROLE rs_global_admin;
[quickstart.cloudera:21000] > GRANT ROLE rs_global_admin TO GROUP recordservice;
[quickstart.cloudera:21000] > GRANT ALL ON SERVER TO ROLE rs_global_admin;
```

#### Running MR Job

Now we can access the path through RecordService. Here we use
[WordCount](src/main/java/com/cloudera/recordservice/examples/mapreduce/WordCount.java),
which counts the total number of words in all files under the path.

**Note**, you may also want to set `HADOOP_CONF_DIR` environment variable before running
the job, to specify properties for the job (e.g., RecordServicePlanner host & port,
Kerberos principal, etc). If the cluster is deployed using Cloudera Manager, then you
can deploy the client configuration first, after which it will generate a configuration
file under `/etc/recordservice/conf`.

```bash
hadoop jar /path/to/recordservice-examples-0.1.jar \
  com.cloudera.recordservice.examples.mapreduce.WordCount \
  "/test-warehouse/tpch.nation" \
  "/tmp/wordcount_output"
```
