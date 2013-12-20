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
  // Collect useful data. 
  sqlParams = csvinfo.getSqlParameters();
  csv_file = sqlParams.get("%%CSV_FILE%%");
  schema = csvinfo.schema;
  table = csvinfo.table;
  hadoop_file = schema + "." + table + ".csv";
  logger.info("Writing file: " + csv_file + " to: " + hadoop_file);

  // Append data to a single file in HDFS. 
  runtime.exec('hadoop fs -put ' + csv_file + ' /user/rhodges/data/' + 
      hadoop_file);
}

