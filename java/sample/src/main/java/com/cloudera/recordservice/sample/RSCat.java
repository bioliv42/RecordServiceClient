package com.cloudera.recordservice.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.thrift.TColumnDesc;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTask;
import com.google.common.base.Joiner;

public class RSCat {
  /**
   * RSCat: a program to cat a file/table similar to hadoop -cat
   *
   * Usage: RSCat file/tablename [number of lines/rows] [--hostname host]
   * [--port port]
   *
   * RSCat works on any file accessible by RecordService. If no number of
   * lines/rows value is provided then RSCat return the whole file.
   */
  static final String USAGE = "Usage: RSCat file/table [number of rows] "
      + "[--hostname host] [--port port]";

  public static List<Object> processRow(Record r, TSchema schema) {
    List<Object> returnList = new ArrayList<Object>();
    for (int i = 0; i < schema.cols.size(); ++i) {
      TColumnDesc col = schema.cols.get(i);
      switch (col.type.type_id) {
      case BOOLEAN:
        returnList.add(new Boolean(r.getBoolean(i)));
        break;
      case TINYINT:
        returnList.add(new Byte(r.getByte(i)));
        break;
      case SMALLINT:
        returnList.add(r.getShort(i));
        break;
      case INT:
        returnList.add(r.getInt(i));
        break;
      case BIGINT:
        returnList.add(r.getLong(i));
        break;
      case FLOAT:
        returnList.add(r.getFloat(i));
        break;
      case DOUBLE:
        returnList.add(r.getDouble(i));
        break;
      case STRING:
        returnList.add(r.getByteArray(i).toString());
        break;
      case VARCHAR:
        returnList.add(r.getByteArray(i).toString());
        break;
      case CHAR:
        returnList.add(r.getByte(i));
        break;
      case TIMESTAMP_NANOS:
        returnList.add(r.getTimestampNanos(i).toTimeStamp());
        break;
      case DECIMAL:
        returnList.add(r.getDecimal(i));
        break;
      default:
        throw new RuntimeException("Service returned type that is not supported. Type = "
            + col.type.type_id);
      }
    }
    return returnList;
  }

  public static void processPath(String path, int numRecords, String hostname, int port)
      throws TRecordServiceException, IOException {
    RecordServicePlannerClient rspc = new RecordServicePlannerClient(hostname, port);
    Request planRequest;
    TPlanRequestResult plannerRequest;
    try {
      planRequest = Request.createPathRequest(path);
      plannerRequest = rspc.planRequest(planRequest);
    } catch (TRecordServiceException rse) {
      // This try catch is used to detect the request type. If the path request
      // fails, we know that path is either a table scan request or doesn't
      // exist
      planRequest = Request.createTableScanRequest(path);
      plannerRequest = rspc.planRequest(planRequest);
    }
    
    Random randGen = new Random();
    RecordServiceWorkerClient rswc;
    for (int i = 0; i < plannerRequest.tasks.size(); ++i) {
      Records rds = null;
      try {
        TTask task = plannerRequest.tasks.get(i);
        int hostChoice = randGen.nextInt(task.local_hosts.size());
        rswc = new RecordServiceWorkerClient.Builder().connect(
            task.local_hosts.get(hostChoice).hostname,
            task.local_hosts.get(hostChoice).port);
        rds = rswc.execAndFetch(task);
        TSchema taskSchema = rds.getSchema();
        Record record;
        while (rds.hasNext()) {
          record = rds.next();
          System.out.println(Joiner.on(",").join(processRow(record, taskSchema)));
        }
      } finally {
        if (rds != null){
          rds.close();
        }
      }
    }
  }

  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println(USAGE);
      return;
    }

    Integer numRecords = Integer.MAX_VALUE;
    int port = 40000;
    String hostname = "localhost";
    String filename = "";
    try {
      for (int i = 0; i < args.length; ++i) {
        if (args[i].equals("--help") || args[i].equals("-h")) {
          System.err.println(USAGE);
          return;
        }
        if (args[i].equals("--hostname") || args[i].equals("-hostname")) {
          hostname = args[i + 1];
          ++i;
        } else if (args[i].equals("--port") || args[i].equals("-p")
            || args[i].equals("-port")) {
          port = Integer.parseInt(args[i + 1]);
          ++i;
        } else if (filename.equals("")) {
          filename = args[i];
        } else {
          numRecords = Integer.parseInt(args[i]);
        }
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      System.err.println("Arguments not formatted correctly.\n" + USAGE);
      return;
    } catch (NumberFormatException nfe) {
      System.err.println("Arguments not formatted correctly.\n" + USAGE);
      return;
    }

    try {
      processPath(filename, numRecords, hostname, port);
    } catch (TRecordServiceException e) {
      System.err.println(e);
      System.exit(1);
    } catch (IOException io) {
      System.err.println(io);
      System.exit(1);
    }
  }
}
