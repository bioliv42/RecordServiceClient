---
title: Using Views to Control Data Access
layout: article
---

RecordService provides column-level security. You can create a view that restricts users in a group to a subset of columns in a dataset. This allows you to maintain a single, secure dataset that can be viewed and updated by users with specific access rights to only the columns they need.

For example, this schema describes a table that stores information about nations.

| column_name | column_type|
|---|---|
| n_nationkey | smallint |
| n_name | string |
| n_regionkey | smallint |
| n_comment | string |

Suppose you have a group of employees who are tasked with editing the nation key and name, but do not need to see the region key and are not supposed to read the comments. You can create a view that allows those employees to see only the two columns you want them to see, without providing access to the others.

To assign access with column-level restrictions, you create a role, assign the role to a group, create a view with the columns the group can access, and then assign the view to the role.

* [Download and Install the RecordService VM]({{site.baseurl}}/vm.html).

* Connect to the RecordService VM using the SSH command `ssh cloudera@quickstart.cloudera`.

* Enter the password `cloudera`.

* Restart Sentry using the following-command line instruction:
```
sudo service sentry-store restart
```
* Create a group named _demogroup_:
```
sudo groupadd demogroup
```
* Add the current user to the group:
```
sudo usermod -a -G demogroup $USER
```
* Currently, you have access to the entire table. Use Impala to select all records from the `tpch.nation` table: 

```
sudo -u $USER impala-shell 
[quickstart.cloudera:21000] > set use_record_service=true
[quickstart.cloudera:21000] > USE tpch;
[quickstart.cloudera:21000] > SELECT * from tpch.nation
Query: select * from tpch.nation
+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+
| n_nationkey | n_name         | n_regionkey | n_comment                                                                                                          |
+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+
| 0           | ALGERIA        | 0           |  haggle. carefully final deposits detect slyly agai                                                                |
| 1           | ARGENTINA      | 1           | al foxes promise slyly according to the regular accounts. bold requests alon                                       |
| 2           | BRAZIL         | 1           | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special         |
| 3           | CANADA         | 1           | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold              |
| 4           | EGYPT          | 4           | y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d                |
| 5           | ETHIOPIA       | 0           | ven packages wake quickly. regu                                                                                    |
| 6           | FRANCE         | 3           | refully final requests. regular, ironi                                                                             |
| 7           | GERMANY        | 3           | l platelets. regular accounts x-ray: unusual, regular acco                                                         |
| 8           | INDIA          | 2           | ss excuses cajole slyly across the packages. deposits print aroun                                                  |
| 9           | INDONESIA      | 2           |  slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull |
| 10          | IRAN           | 4           | efully alongside of the slyly final dependencies.                                                                  |
| 11          | IRAQ           | 4           | nic deposits boost atop the quickly final requests? quickly regula                                                 |
| 12          | JAPAN          | 2           | ously. final, express gifts cajole a                                                                               |
| 13          | JORDAN         | 4           | ic deposits are blithely about the carefully regular pa                                                            |
| 14          | KENYA          | 0           |  pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t                      |
| 15          | MOROCCO        | 0           | rns. blithely bold courts among the closely regular packages use furiously bold platelets?                         |
| 16          | MOZAMBIQUE     | 0           | s. ironic, unusual asymptotes wake blithely r                                                                      |
| 17          | PERU           | 1           | platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun         |
| 18          | CHINA          | 2           | c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos                        |
| 19          | ROMANIA        | 3           | ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account    |
| 20          | SAUDI ARABIA   | 4           | ts. silent requests haggle. closely express packages sleep across the blithely                                     |
| 21          | VIETNAM        | 2           | hely enticingly express accounts. even, final                                                                      |
| 22          | RUSSIA         | 3           |  requests against the platelets use never according to the quickly regular pint                                    |
| 23          | UNITED KINGDOM | 3           | eans boost carefully special requests. accounts are. carefull                                                      |
| 24          | UNITED STATES  | 1           | y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be     |
+-------------+----------------+-------------+--------------------------------------------------------------------------------------------------------------------+
```

* Use Impala to create a restricted view on `tpch.nation`. First you create a role named _demorole_. Next, add the role to the _demogroup_ you created above. Create a view on the n_nationkey and n_name columns of the `tpch.nation` table, and grant the _select_ privilege to demorole.

```
[quickstart.cloudera:21000] > CREATE ROLE demorole;
[quickstart.cloudera:21000] > GRANT ROLE demorole to GROUP demogroup;
[quickstart.cloudera:21000] > CREATE VIEW nation_names AS SELECT n_nationkey, n_name FROM tpch.nation;
[quickstart.cloudera:21000] > GRANT SELECT ON TABLE tpch.nation_names TO ROLE demorole;
```

* Try to select all records from  tpch.nation. The command fails with an authorization exception.

```
[quickstart.cloudera:21000] > select * from tpch.nation;
ERROR: AuthorizationException: User 'cloudera' does not have privileges to execute 'SELECT' on: tpch.nation
```

* Select all records from tpch.nation_names. Impala returns the nation key and nation name columns from the dataset.

```
[quickstart.cloudera:21000] > select * from tpch.nation_names;
Query: select * from tpch.nation_names
+-------------+----------------+
| n_nationkey | n_name         |
+-------------+----------------+
| 0           | ALGERIA        |
| 1           | ARGENTINA      |
| 2           | BRAZIL         |
| 3           | CANADA         |
| 4           | EGYPT          |
| 5           | ETHIOPIA       |
| 6           | FRANCE         |
| 7           | GERMANY        |
| 8           | INDIA          |
| 9           | INDONESIA      |
| 10          | IRAN           |
| 11          | IRAQ           |
| 12          | JAPAN          |
| 13          | JORDAN         |
| 14          | KENYA          |
| 15          | MOROCCO        |
| 16          | MOZAMBIQUE     |
| 17          | PERU           |
| 18          | CHINA          |
| 19          | ROMANIA        |
| 20          | SAUDI ARABIA   |
| 21          | VIETNAM        |
| 22          | RUSSIA         |
| 23          | UNITED KINGDOM |
| 24          | UNITED STATES  |
+-------------+----------------+
Fetched 25 row(s) in 0.59s
```
