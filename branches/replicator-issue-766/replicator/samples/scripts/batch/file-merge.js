/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Demonstration merge script for files.  
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
  logger.info("Writing file: " + csv_file);

  // Append data to a single file. 
  runtime.exec('echo "' + csv_file + '" >> /tmp/output.dat');
  runtime.exec('cat "' + csv_file + '" >> /tmp/output.dat');
}

