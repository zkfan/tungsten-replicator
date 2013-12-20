/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Demonstration connect script for hadoop.  
 */

/**
 * Called once when applier goes online. 
 * 
 * @see com.continuent.tungsten.replicator.applier.batch.JavascriptRuntime
 */
function apply()
{
  // Ensure target directory exists. 
  logger.info("Executing hadoop connect script to create data directory");
  runtime.exec('hadoop fs -mkdir -p /user/rhodges/data');
}
