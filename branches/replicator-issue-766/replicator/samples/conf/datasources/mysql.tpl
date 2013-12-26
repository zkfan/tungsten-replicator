# MySQL pipeline tail datasource. 
replicator.datasource.sink=com.continuent.tungsten.replicator.datasource.SqlDataSource
replicator.datasource.sink.serviceName=${service.name}
replicator.datasource.sink.host=@{APPLIER.REPL_DBHOST}
replicator.datasource.sink.port=@{APPLIER.REPL_DBPORT}
replicator.datasource.sink.user=@{APPLIER.REPL_DBLOGIN}
replicator.datasource.sink.password=@{APPLIER.REPL_DBPASSWORD}
replicator.datasource.sink.schema=${replicator.schema}
replicator.datasource.sink.url=jdbc:mysql:thin://${replicator.datasource.sink.host}:${replicator.datasource.sink.port}/${replicator.schema}?createDB=true
