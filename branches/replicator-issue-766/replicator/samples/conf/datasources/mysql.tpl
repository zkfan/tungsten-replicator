# MySQL datasource. 
replicator.datasource.applier=com.continuent.tungsten.replicator.datasource.SqlDataSource
replicator.datasource.applier.serviceName=${service.name}
replicator.datasource.applier.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.applier.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.applier.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.applier.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.applier.schema=${replicator.schema}
replicator.datasource.applier.url=jdbc:mysql:thin://${replicator.datasource.applier.host}:${replicator.datasource.applier.port}/${replicator.schema}?createDB=true
