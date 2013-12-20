/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Merge script for Infobright.  This script is designed to ensure that
 * the number of rows deleted and inserted exactly matches the number of 
 * deletes and inserts in the CSV file. 
 */

/**
 * Called once when applier goes online. 
 * 
 * @see com.continuent.tungsten.replicator.applier.batch.JavascriptRuntime
 */
function apply()
{
  logger.info("Executing file connect script")
}
