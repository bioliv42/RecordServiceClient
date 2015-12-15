---
layout: article
title: 'RecordService FAQ'
share: false
---
**Q: Where and how are permissions managed?**

**A:** Sentry handles policy metadata. See [Sentry Configuration](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/sg_sentry_service_config.html).

In the current version of Sentry, permissions on views allow for fine-grained access control &mdash; restricting access by column and row.

**Q: What kind of data can we use RecordService for?**

**A:** Hive Metastore Tables only. Support might be added in the future for other schema sources, depending on customer demand.

**Q: What happens if you try to access data controlled by RecordService without using RecordService?** 

**A:** Sentry’s HDFS Sync feature ensures that the files are locked such that only users with full access to all of the values in a file (with Sentry permissions for the entire table) are allowed to read the files directly. See [Configuring the Sentry Service](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/sg_sentry_service_config.html).

**Q: Do I need to be running Sentry to evaluate the RecordService?**

**A:** No. You can deploy RecordService without Sentry, in which case no authorization checks will happen. This can be useful to evaluate functionality and performance.

**Q: Are there any APIs to discover the permissions that are set?** 

**A:** Hue can show this, as well as SHOW commands in Hive or Impala CLI.

**Q: What is the RecordService security model? Can you purposely restrict views into the data?**

**A:** Yes, you can control permissions per view. Setting the active role, is not currently supported.

**Q: Why does RecordService implement its own schema (which seems to be a copy of Hive's schema)?**

**A:** The client API is layered so that it does not have to pull in all dependencies. As you move higher in the client API, you get access to more standard Hadoop objects. In this case, you get a recordservice-hive JAR that returns Hive Schema objects.

**Q: Can you list tables through RecordService, or do you use the Hive metastore to get tables, and then ask RecordService for the schema?**

**A:** You cannot list tables through RecordService. You have to use the Hive Metastore, or use a tool such as Hue.

You need only the fully qualified table name to read from a table, so the client does not need to pass the table metadata to RS.

**Q: How does accessing a path directly compare to querying the Hive metastore?**

**A:** RecordService infers the schema. If the path contains a self-describing file, such as Avro or Parquet, it uses that. For files like CSV, RecordService defaults to a STRING schema.
In a future release RecordService might infer schema from files, but the security rules for paths are still under consideration. For now, RecordService only supports reading from tables defined in the Hive Metastore.
