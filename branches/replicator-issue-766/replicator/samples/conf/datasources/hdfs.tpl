# HDFS data source. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.HdfsDataSource
replicator.datasource.applier.serviceName=${service.name}
# Storage location for replication catalog data. 
replicator.datasource.applier.directory=/user/tungsten/metadata

# HDFS-specific information. 
replicator.datasource.applier.hdfsUri=hdfs://cloudera44:8020/user/tungsten/metadata
replicator.datasource.applier.hdfsConfigProperties=${replicator.home.dir}/conf/hdfs-config.properties
