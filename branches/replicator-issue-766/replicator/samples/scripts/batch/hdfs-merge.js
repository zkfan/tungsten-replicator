/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Demonstration merge script for Hadoop.  
 */

/**
 * Called once for each table that must be loaded. 
 * 
 * @see com.continuent.tungsten.replicator.applier.batch.SqlWrapper
 * @see com.continuent.tungsten.replicator.applier.batch.CsvInfo
 * @see com.continuent.tungsten.replicator.applier.batch.JavascriptRuntime
 */
function apply(csvinfo)
{
  // Assemble the parts of the file. 
  sqlParams = csvinfo.getSqlParameters();
  csv_file = sqlParams.get("%%CSV_FILE%%");
  hive_base = "/user/tungsten/data"
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.seqno;
  hive_path = schema + "/" + table + "/" + table + "-" + seqno + ".csv";
  hadoop_file = hive_base + "/" + hive_path;
  logger.info("Writing file: " + csv_file + " to: " + hadoop_file);

  // Copy the file into HDFS. 
  hdfs.put(csv_file, hadoop_file);
}
