# File datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.FileDataSource
replicator.datasource.applier.serviceName=${service.name}

# Storage location for replication catalog data. 
replicator.datasource.applier.directory=${replicator.home.dir}/data

# CSV generation configuration settings.  
replicator.datasource.applier.csv=com.continuent.tungsten.common.csv.CsvSpecification
replicator.datasource.applier.csv.fieldSeparator=,
replicator.datasource.applier.csv.RecordSeparator=\\n
replicator.datasource.applier.csv.useQuotes=true
