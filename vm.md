---
layout: article
title: 'Using the RecordService VM'
---

{% include toc.html %}

## Downloading RecordService VM

Follow these steps to download the RecordService VM.

1. Install VirtualBox. The VM works with VirtualBox version 4.3 on Ubuntu 14.04 and VirtualBox version 5 on OSX 10.9. Download VirtualBox for free at [https://www.virtualbox.org/wiki/Downloads](https://www.virtualbox.org/wiki/Downloads).
1. Clone the recordservice-quickstart repository onto your local disk from [https://github.com/cloudera/recordservice-quickstart](https://github.com/cloudera/recordservice-quickstart).

## Installing the RecordService VM

|| <b>Note:</b> If you have previously installed the VM on your host machine, follow the instructions in [Verify VM is Listed Correctly in Hosts](#verify-vm-is-listed-correctly-in-hosts) and [Verify Known Hosts](#verify-known-hosts). ||

Follow these steps to install the RecordService VM.

1. In a terminal window, navigate to the root of the RSQuickstart Git repository.
1. Run the script `install.sh`.

This script downloads an `ova` file and loads it into VirtualBox. The script might ask you to enter a password, because it edits your `/etc/hosts` file to give the VM a stable IP address, `quickstart.cloudera`. When the script completes, the running VM functions as a RecordService server.

### Testing Your VM Configuration

Test that the VM is running and IP forwarding is configured properly.

1. Enter the command `ssh cloudera@quickstart.cloudera`.
1. Enter the password `cloudera`.

If you cannot ssh to the VM, see [Troubleshooting the VM Configuration](#troubleshooting-the-vm-configuration).

Successfully connecting through ssh verifies that you can log in to the VM.

### Configuring RecordService Environment Variables

1. On your host machine, navigate to the root of your RecordServiceClient repository, `$RECORD_SERVICE_HOME`.
1. Run `source config.sh`.
1. Navigate to the `recordservice-quickstart` directory.
1. Run `source vm_env.sh`.
1. Navigate to `$RECORD_SERVICE_HOME/java`.
1. Test your environment with the command `mvn test -DargLine="-Duser.name=recordservice"`

The VM is preconfigured with sample data to execute the tests. The _recordservice_ user has access to the data via Sentry.

The VM is not secured with LDAP or Kerberos. If you want to change the security configuration, you can add roles in Sentry through `impala-shell`. If you have `impala-shell` on your host machine, you can connect to the VM by issuing the following command.

```
impala-shell -i quickstart.cloudera:21000 -u impala
```

You can also connect from within `impala-shell`.

```
CONNECT quickstart.cloudera:21000;
```

## Running Examples on the VM

The following examples demonstrate how to use RecordService to implement column- and row-level access in Hadoop. Additional examples are described in the topic [RecordService Examples]({{site.baseurl}}/examples).

### Example: Using RecordService to Control Column-level Access

RecordService provides column-level security. You can restrict users in a group to a subset of columns in a dataset. This allows you to maintain a single, secure dataset that can be viewed and updated by users with specific access rights to only the columns they need.

For example, this schema describes a table that stores information about employees.

| column_name | column_type|
|---|---|
| firstname | string |
| lastname | string |
| position | string |
| department | string |
| salary | long |
| phone | long |

Suppose you want your employees to have access to the names and phone numbers, but not the position or salary info. You can create a group that allows users to see only the columns you want them to see.

To assign access with column-level restrictions, you create a role, assign the role to a group, and then grant permissions to the role.

* Connect to the RecordService VM using the SSH command `ssh cloudera@quickstart.cloudera`.

* Enter the password `cloudera`.

* Restart Sentry using the following-command line instruction:
```
sudo service sentry-store restart
```

* Currently, you have access to the entire table. Use Impala to select all records from the `rs.employees` table:

<pre>
$ impala-shell
[quickstart.cloudera:21000] > use rs;
Query: use rs
[quickstart.cloudera:21000] > select * from rs.employees;
Query: select * from rs.employees
+-----------+-------------+-----------------------------------------------+------------------+--------+-------------+
| firstname | lastname    | position                                      | department       | salary | phonenumber |
+-----------+-------------+-----------------------------------------------+------------------+--------+-------------+
| Peter     | Aaron       | WATER RATE TAKER                              | WATER MGMNT      | 88968  | 5551962     |
| Rufi      | Bhola       | TRAFFIC SIGNAL REPAIRMAN                      | TRANSPORTN       | 95888  | 5551543     |
| Faruk     | Bota        | POLICE OFFICER                                | POLICE           | 80778  | 5551860     |
| Mary      | Bradley     | POLICE OFFICER                                | POLICE           | 80778  | 5551638     |
| Joseph    | Chu         | ASST TO THE ALDERMAN                          | CITY COUNCIL     | 70764  | 5551218     |
| Sam       | Cinq        | CHIEF CONTRACT EXPEDITER                      | GENERAL SERVICES | 84780  | 5551969     |
| Carol     | Cloud       | CIVIL ENGINEER IV                             | WATER MGMNT      | 104736 | 5551219     |
| Mackay    | Dalford     | POLICE OFFICER                                | POLICE           | 46206  | 5551516     |
| Drake     | Desmond     | GENERAL LABORER - DSS                         | STREETS & SAN    | 40560  | 5551551     |
| Sachen    | Dhoot       | ELECTRICAL MECHANIC                           | AVIATION         | 91520  | 5551500     |
| Albert    | Encino      | FIRE ENGINEER                                 | FIRE             | 90456  | 5551834     |
| Arthur    | Excalibur   | POLICE OFFICER                                | POLICE           | 86520  | 5551485     |
| Eric      | Freeman     | FOSTER GRANDPARENT                            | FAMILY & SUPPORT | 2756   | 5551706     |
| Fred      | Gobi        | CLERK III                                     | POLICE           | 43920  | 5551597     |
| Ivan      | Gorbachev   | INVESTIGATOR - IPRA II                        | IPRA             | 72468  | 5551978     |
| Theodore  | Henry       | POLICE OFFICER                                | POLICE           | 69684  | 5551904     |
| Cranston  | Horton      | POLICE OFFICER                                | POLICE           | 80778  | 5551924     |
| Bobi      | Ingerwen    | FIREFIGHTER (PER ARBITRATORS AWARD)-PARAMEDIC | FIRE             | 98244  | 5551294     |
| Franz     | Isenglass   | POLICE OFFICER                                | POLICE           | 80778  | 5551249     |
| Jenny     | Jakuti      | FIREFIGHTER/PARAMEDIC                         | FIRE             | 87720  | 5551656     |
| Karl      | Karloff     | ENGINEERING TECHNICIAN VI                     | WATER MGMNT      | 106104 | 5551911     |
| Kathryn   | Kretchmer   | FIREFIGHTER-EMT                               | FIRE             | 91764  | 5551875     |
| Loren     | Lakshmi     | LIEUTENANT                                    | FIRE             | 110370 | 5551082     |
| Dudley    | Less        | SENIOR ENVIRONMENTAL INSPECTOR                | HEALTH           | 76656  | 5551407     |
| Mahood    | Mahmut      | CROSSING GUARD                                | POLICE           | 16692  | 5551892     |
| Wanda     | Myers       | GENERAL LABORER - DSS                         | STREETS & SAN    | 40560  | 5551644     |
| Trey      | Mystique    | ELECTRICAL MECHANIC-AUTO-POLICE MTR MNT       | GENERAL SERVICES | 91520  | 5551376     |
| Manuel    | Nickels     | POLICE OFFICER                                | POLICE           | 86520  | 5551380     |
| Sheldon   | Overton     | PARAMEDIC                                     | FIRE             | 54114  | 5551551     |
| Lana      | Park        | MOTOR TRUCK DRIVER                            | STREETS & SAN    | 71781  | 5551530     |
| Franny    | Periodico   | LIBRARY ASSOCIATE - HOURLY                    | PUBLIC LIBRARY   | 24835  | 5551413     |
| Perry     | Polite      | CIVIL ENGINEER IV                             | WATER MGMNT      | 104736 | 5551891     |
| Mike      | Processer   | SENIOR PROGRAMMER/ANALYST                     | DoIT             | 104736 | 5551139     |
| Quincy    | Quintado    | ENGINEERING TECHNICIAN V                      | BUSINESS AFFAIRS | 96672  | 5551406     |
| Richard   | Ramstadt    | POLICE OFFICER                                | POLICE           | 46206  | 5551517     |
| Chandra   | Sambrosa    | SENIOR COMPANION                              | FAMILY & SUPPORT | 2756   | 5551896     |
| Amber     | Sikh        | SUPERVISING TRAFFIC CONTROL AIDE              | OEMC             | 55800  | 5551384     |
| Thomas    | Spelt       | SANITATION LABORER                            | STREETS & SAN    | 72384  | 5551807     |
| Boli      | Tiku        | POLICE OFFICER                                | POLICE           | 92316  | 5551921     |
| Clark     | Trent       | AMBULANCE COMMANDER                           | FIRE             | 123948 | 5551998     |
| Noreen    | Umbrella    | POOL MOTOR TRUCK DRIVER                       | STREETS & SAN    | 16151  | 5551173     |
| Conrad    | Valvoly     | FIREFIGHTER-EMT                               | FIRE             | 85680  | 5551908     |
| June      | Vendi       | SEWER BRICKLAYER                              | WATER MGMNT      | 88566  | 5551022     |
| Auicula   | Ventricular | TREE TRIMMER                                  | STREETS & SAN    | 74464  | 5551512     |
| Solomon   | Wally       | POLICE OFFICER                                | POLICE           | 89718  | 5551750     |
| Winifred  | Wonderman   | ELECTRICAL MECHANIC                           | AVIATION         | 91520  | 5551990     |
| Elvin     | Yahuli      | POLICE OFFICER                                | POLICE           | 86520  | 5551675     |
| Aloysius  | Zeke        | FIREFIGHTER-EMT                               | FIRE             | 95460  | 5551601     |
| Anderson  | Zephyr      | PERSONAL COMPUTER OPERATOR III                | HEALTH           | 66684  | 5551717     |
| Zelda     | Zion        | FIREFIGHTER-EMT                               | FIRE             | 99920  | 5553970     |
+-----------+-------------+-----------------------------------------------+------------------+--------+-------------+
Fetched 50 row(s) in 3.75s
</pre>

* Use Impala to set permissions for a group of users of `rs.employees`. First,  create a role named _demorole_. Next, add the role to the _demogroup_ you created before starting Impala. Grant the _select_ privilege to demorole for only the columns `firstname`, `lastname`, and `phonenumber` from the `rs.employees` table.

<pre>
[quickstart.cloudera:21000] > create role demorole;
Query: create role demorole

Fetched 0 row(s) in 0.40s
[quickstart.cloudera:21000] > grant role demorole to group demogroup;
Query: grant role demorole to group demogroup

[quickstart.cloudera:21000] > GRANT SELECT(firstname, lastname, phonenumber) ON TABLE rs.employees TO ROLE demorole;

Fetched 0 row(s) in 0.11s
</pre>

* Exit Impala.

* From the home directory, execute the following command. This is a trivial example class that counts the number of records in the table. Since the command specifies the <i>salary</i> column, to which the <i>demouser</i> does not have access, the command fails.

<pre>
[cloudera@quickstart ~]$ sudo su demouser hadoop jar \
./recordservice-client-0.2.0-cdh5.5.x/lib/recordservice-examples-0.2.0-cdh5.5.x.jar \
com.cloudera.recordservice.examples.mapreduce.RecordCount \
"select lastname, salary from rs.employees" "/tmp/count_salary_output"

15/12/08 17:41:26 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032

. . .

RecordServiceException: TRecordServiceException(code:INVALID_REQUEST, message:Could not plan request., detail:AuthorizationException: User 'cloudera' does not have privileges to execute 'SELECT' on: rs.employees
. . .
</pre>

* Now run the same command, but specify the `firstname`, `lastname`, and `phonenumber` columns.

<pre>
[cloudera@quickstart ~]$ sudo su demouser hadoop jar \
./recordservice-client-0.2.0-cdh5.5.x/lib/recordservice-examples-0.2.0-cdh5.5.x.jar \
com.cloudera.recordservice.examples.mapreduce.RecordCount \
"select firstname, lastname, phonenumber from rs.employees" \
"/tmp/count_phonelist_output"

15/12/08 17:42:25 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
. . .
File Output Format Counters
    Bytes Written=3
</pre>

* View the results using Hadoop.
<pre>
hadoop fs -cat /tmp/count_phonelist_output/part-r-00000
</pre>

The result returned is a row count of 50.

### Example: Using RecordService to Control Row-level Access

You can also define a view and assign access that restricts a user to certain rows in the data set.

Create a view that restricts the rows returned. For example, where <i>position</i> is not <code>POLICE OFFICER</code>.

<pre>
[quickstart.cloudera:21000] > use rs;
[quickstart.cloudera:21000] > create view rs.no_police as select * from rs.employees where position <> "POLICE OFFICER";
Query: create view rs.no_police as select * from rs.employees where position <> "POLICE OFFICER"
</pre>

Assign that view to <i>demorole</i>.

<pre>
[quickstart.cloudera:21000] > grant select on table rs.no_police to role demorole;
Query: grant select on table rs.no_police to role demorole
</pre>
* Run a query against the rs.no_police view.

<pre>
[cloudera@quickstart ~]$ sudo su demouser hadoop jar \
./recordservice-client-0.2.0-cdh5.5.x/lib/recordservice-examples-0.2.0-cdh5.5.x.jar \
com.cloudera.recordservice.examples.mapreduce.RecordCount \
"select firstname, lastname, phonenumber from rs.no_police" \
"/tmp/count_no_police_output"

15/12/08 17:50:18 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
. . .
File Output Format Counters
    Bytes Written=3
</pre>

* View the results using Hadoop.
<pre>
hadoop fs -cat /tmp/count_no_police_output/part-r-00000
</pre>

The result returned is a row count of 38.

Additional examples are described in the [examples]({{site.baseurl}}/examples) topic.

## Troubleshooting the VM Configuration

### Unable to ssh to the VM
* Ensure that the ssh daemon is running on your machine.
* Ensure that the RecordService VM is running. In your terminal, enter the following command:

```
VBoxManage list runningvms
```

&nbsp;&nbsp;&nbsp;&nbsp;You should see “rs-demo” listed as a running VM.

### Verify VM is Listed Correctly in Hosts

Check that the VM is listed correctly in your `/etc/hosts` file. If you open the file, you should see a line that lists an IP address followed by `quickstart.cloudera`. You can check the VM’s IP with the following command:

```
VBoxManage guestproperty get rs-demo /VirtualBox/GuestInfo/Net/0/V4/IP
```

### Verify Known Hosts

If you’ve used a Cloudera QuickStart VM before, your known hosts file might already have an entry for `quickstart.cloudera` registered to a different key. Delete any reference to `quickstart.cloudera` from your known hosts file, which is usually found in `~/.ssh/known_hosts`.

### Verify Workers Are Running

If you receive an error message similar to the following, your worker nodes are likely not running:
<pre>
Exception in thread "main" java.io.IOException:
com.cloudera.recordservice.core.RecordServiceException:
TRecordServiceException(code:INVALID_REQUEST, message:
Worker membership is empty. Please ensure all RecordService Worker nodes are running.)
</pre>

You can correct the problem by restarting the RecordService server using the following command:

```
sudo service recordservice-server restart
```

## Debugging the VM

### Restarting a service

To restart a service, use the standard RHEL service model.

```
service <service-name> start|stop|restart
```

You can view all of the installed services `/etc/init.d` directory.

### Debugging Via Logs

RecordService logs are in the `/var/log/recordservice` directory. You can find most service logs in the `/var/log` directory.

### Dubugging Via Webpage

View the RecordService debug page on your host machine at `quickstart.cloudera:11050`.

### Other Service Variables

To view the default execution environment for a service, look for its file in the `/etc/default` directory.
