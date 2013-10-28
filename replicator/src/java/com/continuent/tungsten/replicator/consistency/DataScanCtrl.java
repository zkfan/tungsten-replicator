/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.consistency;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.management.remote.JMXConnector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.common.exec.ArgvIterator;
import com.continuent.tungsten.common.jmx.JmxManager;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.ddlscan.DDLScanCtrl;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean;
import com.continuent.tungsten.replicator.management.tungsten.TungstenPlugin;
import com.continuent.tungsten.replicator.thl.ProtocolParams;

/**
 * This class defines a DataScanCtrl that implements a utility to access
 * DataScan methods. See the printHelp() command for a description of current
 * commands.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DataScanCtrl
{
    private static Logger              logger           = Logger.getLogger(DataScanCtrl.class);

    private ArgvIterator               argvIterator;

    /** true - direct check, false - replicated. */
    private boolean                    checkDirect      = false;

    private String                     rmiHostMaster    = null;
    private String                     rmiPortMaster    = null;
    private String                     jdbcUserMaster   = null;
    private String                     jdbcPassMaster   = null;
    private String                     jdbcUrlMaster    = null;
    private String                     service          = null;
    private String                     serviceSchema    = null;
    private String                     tables           = null;
    private String                     configFile       = null;

    private boolean                    verbose          = false;
    private boolean                    printValues      = false;

    private boolean                    methodPk         = true;
    private String                     schema           = null;

    /** How long to wait for consistency check to arrive to the slave. */
    private int                        checkTimeout     = 30;

    // TODO: add automated calculation based on table size?
    int                                chunkSize        = 8192;
    private int                        granularity      = 1;

    /** Where to start scanning the table. Used with a single table. */
    private long                       rowFrom          = ConsistencyTable.ROW_UNSET;
    /** Where to stop scanning the table. Used with a single table. */
    private long                       rowTill          = ConsistencyTable.ROW_UNSET;

    /** JMX connection to the master service. */
    private OpenReplicatorManagerMBean master           = null;

    /** Connection to the master database (user schema). */
    private Database                   masterDbUser     = null;

    /** JMX connections to the slave services. */
    private List<Map<String, String>>  slaves           = null;

    /** Connections to the slave databases (Tungsten schema). */
    private Database[]                 slaveDbTungsten  = null;

    /** JDBC URL addresses of the slaves. */
    private String[]                   jdbcUrlSlave     = null;

    /** Consistency table (used with direct checks). */
    private Table                      consistencyTable = null;

    DataScanCtrl(String[] argv)
    {
        argvIterator = new ArgvIterator(argv);
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        DataScanCtrl ctrl = new DataScanCtrl(argv);
        ctrl.go();
    }

    /**
     * Parse and validate command line arguments.
     */
    private void parseArgs()
    {
        String command = null;
        String outFile = null;
        String renameDefinitions = null;

        String curArg = null;
        while (argvIterator.hasNext())
        {
            curArg = argvIterator.next();
            if ("-check".equals(curArg))
            {
                if (argvIterator.hasNext())
                {
                    String m = argvIterator.next();
                    if (m.equalsIgnoreCase("direct"))
                        checkDirect = true;
                    else if (m.equalsIgnoreCase("replicated"))
                        checkDirect = false;
                    else
                        fatal("Unrecognised check (use direct or replicated instead): "
                                + m, null);
                    println(String.format("Check: " + m));
                }
            }
            else if ("-method".equals(curArg))
            {
                if (argvIterator.hasNext())
                {
                    String m = argvIterator.next();
                    if (m.equalsIgnoreCase("pk"))
                        methodPk = true;
                    else if (m.equalsIgnoreCase("limit"))
                        methodPk = false;
                    else
                        fatal("Unrecognised method (use pk or limit instead): "
                                + m, null);
                    println(String.format("Method: " + m));
                }
            }
            else if ("-rmiHost".equals(curArg))
            {
                if (argvIterator.hasNext())
                    rmiHostMaster = argvIterator.next();
            }
            else if ("-rmiPort".equals(curArg))
            {
                if (argvIterator.hasNext())
                    rmiPortMaster = argvIterator.next();
            }
            else if ("-service".equals(curArg))
            {
                if (argvIterator.hasNext())
                {
                    service = argvIterator.next();
                    serviceSchema = "tungsten_" + service;
                }
            }
            else if ("-db".equals(curArg))
            {
                if (argvIterator.hasNext())
                    schema = argvIterator.next();
            }
            else if ("-tables".equals(curArg))
            {
                if (argvIterator.hasNext())
                    tables = argvIterator.next();
            }
            else if ("-timeout".equals(curArg))
            {
                if (argvIterator.hasNext())
                    checkTimeout = Integer.parseInt(argvIterator.next());
            }
            else if ("-chunk".equals(curArg))
            {
                if (argvIterator.hasNext())
                {
                    String val = argvIterator.next();
                    if ("single".equalsIgnoreCase(val))
                        chunkSize = -1;
                    else
                    {
                        chunkSize = Integer.parseInt(val);
                        if (!((chunkSize & -chunkSize) == chunkSize))
                        {
                            int x = findClosestChunk(chunkSize);
                            fatal("-chunk must specify a value of power of two (eg. 1024, 2048, ...), try "
                                    + x, null);
                        }
                    }
                }
            }
            else if ("-granularity".equals(curArg))
            {
                if (argvIterator.hasNext())
                    granularity = Integer.parseInt(argvIterator.next());
            }
            else if ("-from".equals(curArg))
            {
                if (argvIterator.hasNext())
                    rowFrom = Integer.parseInt(argvIterator.next());
                if (rowFrom < 0)
                    fatal("-from option requires a non-negative number", null);
            }
            else if ("-till".equals(curArg))
            {
                if (argvIterator.hasNext())
                    rowTill = Integer.parseInt(argvIterator.next());
                if (rowTill < 0)
                    fatal("-till option requires a non-negative number", null);
            }
            else if ("-verbose".equals(curArg))
            {
                verbose = true;
            }
            else if ("-values".equals(curArg))
            {
                printValues = true;
            }
            else if ("-conf".equals(curArg))
            {
                if (argvIterator.hasNext())
                    configFile = argvIterator.next();
            }
            else if ("-user".equals(curArg))
            {
                if (argvIterator.hasNext())
                    jdbcUserMaster = argvIterator.next();
            }
            else if ("-pass".equals(curArg))
            {
                if (argvIterator.hasNext())
                    jdbcPassMaster = argvIterator.next();
            }
            else if ("-url1".equals(curArg))
            {
                if (argvIterator.hasNext())
                    jdbcUrlMaster = argvIterator.next();
            }
            else if ("-url2".equals(curArg))
            {
                if (argvIterator.hasNext())
                {
                    jdbcUrlSlave = new String[1];
                    jdbcUrlSlave[0] = argvIterator.next();
                }
            }
            else if ("-out".equals(curArg))
            {
                if (argvIterator.hasNext())
                    outFile = argvIterator.next();
            }
            else if ("-rename".equals(curArg))
            {
                if (argvIterator.hasNext())
                    renameDefinitions = argvIterator.next();
            }
            else if ("-help".equals(curArg))
            {
                printHelp();
                succeed();
            }
            else if (curArg.startsWith("-"))
            {
                println("Unrecognized option: " + curArg);
                printHelp();
                fail();
            }
            else
                command = curArg;
        }

        if (schema == null)
            fatal("Database is not provided! Use -db parameter.", null);
    }

    /**
     * Returns closest (bigger) valid chunk size to the given one. Valid is the
     * one that is the power of two.
     */
    private int findClosestChunk(int chunkSize)
    {
        int x = 1;
        for (; x <= chunkSize; x = x * 2)
            ;
        return x;
    }

    private void initMasterConnections() throws Exception
    {
        // Try to get connectivity information.
        if (jdbcUserMaster == null || jdbcPassMaster == null
                || jdbcUrlMaster == null)
        {
            // Credentials not provided, retrieve from configuration.
            if (configFile == null)
            {
                if (service == null)
                {
                    // Nothing is given. Retrieve configuration file of
                    // default service.
                    configFile = DDLScanCtrl.lookForConfigFile();
                    if (configFile == null)
                    {
                        fatal("You must specify either a config file or a service name (-conf or -service)",
                                null);
                    }
                }
                else
                {
                    // Retrieve configuration file of a given service.
                    ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                            .getConfiguration(service);
                    configFile = runtimeConf.getReplicatorProperties()
                            .getAbsolutePath();
                }
            }

            // Read connection info from configuration file.
            if (configFile != null)
                println(String.format("Using configuration: %s", configFile));

            TungstenProperties properties = DDLScanCtrl.readConfig(configFile);
            if (properties != null)
            {
                // Substitute DBNAME.
                jdbcUrlMaster = substituteDbName(
                        properties.getString(ReplicatorConf.RESOURCE_JDBC_URL),
                        schema);

                // Get login.
                jdbcUserMaster = properties
                        .getString(ReplicatorConf.GLOBAL_DB_USER);
                jdbcPassMaster = properties
                        .getString(ReplicatorConf.GLOBAL_DB_PASSWORD);

                if (jdbcUserMaster == null || jdbcUrlMaster == null)
                    throw new ReplicatorException(
                            "Configuration file doesn't have JDBC URL credentials");
            }
            else
                throw new ReplicatorException(
                        "Unable to read configuration file " + configFile);
        }

        if (!checkDirect)
        {
            // Connect to the master.
            master = getOpenReplicator(rmiHostMaster, rmiPortMaster, service);
            if (master == null)
                fatal("Unable to connect to master Replicator", null);
        }

        // Connection to the master DB user schema.
        masterDbUser = connectDB(jdbcUrlMaster, jdbcUserMaster, jdbcPassMaster);
    }

    /**
     * Connects to the specified DBMS and prints status.
     */
    private Database connectDB(String url, String user, String pass)
            throws SQLException
    {
        Database connDB = DatabaseFactory.createDatabase(url, user, pass);
        connDB.connect();
        println(String.format("Connected to %s: %s", connDB.getType()
                .toString(), url));
        return connDB;
    }

    private void initSlaveConnections() throws Exception
    {
        if (checkDirect)
        {
            // Establish DBMS connection to the slave from user parameters.
            if (jdbcUrlSlave == null)
                fatal("Direct check selected, but no slave DBMS credentials provided",
                        null);

            // TODO: enable more than one slave checking with direct method.
            slaveDbTungsten = new Database[1];
            slaveDbTungsten[0] = connectDB(jdbcUrlSlave[0], jdbcUserMaster, jdbcPassMaster);
        }
        else
        {
            // Find slaves from master Replicator.
            slaves = master.getClients();
            if (slaves == null)
                throw new ReplicatorException("No slaves to check found");
            OpenReplicatorManagerMBean[] slave = new OpenReplicatorManagerMBean[slaves
                    .size()];
            slaveDbTungsten = new Database[slaves.size()];
            jdbcUrlSlave = new String[slaves.size()];
            for (int c = 0; c < slaves.size(); c++)
            {
                // Connect to JMX.
                Map<String, String> client = slaves.get(c);
                String rmiHostSlave = client.get(ProtocolParams.RMI_HOST);
                String rmiPortSlave = client.get(ProtocolParams.RMI_PORT);
                slave[c] = getOpenReplicator(rmiHostSlave, rmiPortSlave,
                        service);

                // Connect to DBMS and Tungsten schema.
                // TODO: support for different DB password for slaves.
                jdbcUrlSlave[c] = slave[c]
                        .properties(ReplicatorConf.THL_DB_URL).get(
                                ReplicatorConf.THL_DB_URL);
                slaveDbTungsten[c] = connectDB(jdbcUrlSlave[c], jdbcUserMaster,
                        jdbcPassMaster);
            }
        }
    }

    /**
     * Checks table definition and whether it is safe to use with the
     * consistency check method chosen.
     * 
     * @return true, if it is safe.
     */
    private boolean validateTable(Table table)
    {
        if (methodPk)
        {
            if (table.getPrimaryKey() != null
                    && table.getPrimaryKey().getColumns().size() == 1
                    && !isNumericCol(table.getPrimaryKey().getColumns().get(0)))
            {
                println("WARNING: " + table.getName()
                        + " has a single-column PK, but it's not numeric");
                return false;
            }
        }
        else
        {
            if (table.getPrimaryKey() != null
                    && table.getPrimaryKey().getColumns().size() == 1
                    && isNumericCol(table.getPrimaryKey().getColumns().get(0)))
            {
                println("WARNING: "
                        + table.getName()
                        + " has a single-column numeric PK - "
                        + "it is highly recommended to use PK-based consistency check"
                        + " as opposed to the LIMIT one. Use: -method pk");
                return false;
            }
        }
        return true;
    }

    /**
     * Returns string representation of the chosen check method.
     */
    private String getMethod()
    {
        if (methodPk)
            return ConsistencyCheck.Method.MD5PK;
        else
            return ConsistencyCheck.Method.MD5;
    }

    /**
     * Process commands.
     */
    public void go()
    {
        try
        {
            parseArgs();
            initMasterConnections();
            initSlaveConnections();

            // TODO: refactor for multiple tables!

            if (service != null)
                println("Service: " + service);
            
            println("Database: " + schema);
            println("Table(s): " + tables);

            Table table = masterDbUser.findTable(schema, tables);
            if (table == null)
                fatal("Table not found (note: multiple tables not supported yet)",
                        null);
            if (printValues)
            {
                println("Columns:");
                printColumns(table);
            }
            validateTable(table);

            // Determine begin and end position.
            if (rowFrom >= 0 && rowTill > 0)
            {
                String usePk = "";
                if (methodPk)
                    usePk = " (PK)";
                println("Row from" + usePk + ": " + rowFrom);
                println("Row till" + usePk + ": " + rowTill);
            }
            else if (methodPk)
            {
                rowFrom = retrieveMaxMinPK(masterDbUser, table, false);
                rowTill = retrieveMaxMinPK(masterDbUser, table, true);
                println("Row from (min PK): " + rowFrom);
                println("Row till (max PK): " + rowTill);
            }
            else if (!methodPk)
            {
                rowFrom = 0;
                rowTill = retrieveRowCount(masterDbUser, table);
                println("Row from: " + rowFrom);
                println("Row till (count): " + rowTill);
            }

            // Print actual count of rows.
            if (methodPk)
            {
                println("Rows in-between: "
                        + retrieveRowCount(masterDbUser, table, rowFrom,
                                rowTill));
            }

            if (chunkSize == -1)
            {
                // User asked for a single-chunk pass.
                chunkSize = findClosestChunk((int) (rowTill - rowFrom));
            }
            println("Chunk size: " + chunkSize);
            println("Granularity: " + granularity);
            
            if (checkDirect)
            {
                // Find consistency table to be used later on.
                consistencyTable = TungstenPlugin.findConsistencyTable(
                        masterDbUser, serviceSchema);
            }

            printvln("Checking (sequentially):");
            for (long r = rowFrom; r < rowTill; r += chunkSize)
            {
                // Issue the check.
                int id = consistencyCheck(table, r, chunkSize);

                // See results on the slaves.
                for (int c = 0; c < slaveDbTungsten.length; c++)
                {
                    boolean consistent = didCheckPass(slaveDbTungsten[c], id);
                    if (consistent)
                        print("-");
                    if (!consistent)
                    {
                        String host = slaveDbTungsten[c].getDatabaseMetaData()
                                .getURL();
                        if (!checkDirect)
                            host = slaves.get(c).get(ProtocolParams.RMI_HOST);
                        println("x");
                        printvln("Inconsistent chunk @ " + host + ": row=" + r
                                + " range=" + chunkSize + " check=" + id);
                        printvln("Drilling down (binary search):");
                        drillDown(table, r, chunkSize, slaveDbTungsten[c], host);
                        printvln("");
                        printvln("Continuing check:");
                    }
                }

                printProgress(r, rowFrom, rowTill);
            }
            println("");
            println("Checking completed.");
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    /**
     * Render (print) progress bar.
     */
    private void printProgress(long row, long rowFrom, long rowTill)
    {
        long rowsProcessed = row + chunkSize - rowFrom;
        if (rowsProcessed > (rowTill - rowFrom))
            rowsProcessed = (rowTill - rowFrom);
        print(String
                .format("%d%%",
                        (int) (((double) (rowsProcessed) / (double) (rowTill - rowFrom)) * 100.0f)));
    }

    /**
     * Returns if the given column is of numeric type.
     */
    public static boolean isNumericCol(Column col)
    {
        int t = col.getType();
        return t == Types.BIGINT || t == Types.DECIMAL || t == Types.DOUBLE
                || t == Types.FLOAT || t == Types.INTEGER || t == Types.NUMERIC
                || t == Types.REAL || t == Types.SMALLINT || t == Types.TINYINT;
    }

    /**
     * Executes a direct or replicated consistency check, depending on the
     * current user choice.
     * 
     * @param table Table to check.
     * @param row Row to start.
     * @param range Limit of rows.
     * @return ID of the issued check or -1 on problems.
     */
    private int consistencyCheck(Table table, long row, long range)
            throws Exception
    {
        int id = -1;
        if (checkDirect)
        {
            // Issue the check directly into the database.
            id = TungstenPlugin.findNextConsistencyId(masterDbUser,
                    consistencyTable);
            // TODO: make check column names and types a dynamic choice.
            ConsistencyCheck cc = ConsistencyCheckFactory
                    .createConsistencyCheck(id, table, (int) row, (int) range,
                            getMethod(), true, false);
            masterDbUser.consistencyCheck(consistencyTable, cc);
        }
        else
        {
            // Issue the check through master Replicator.
            id = master.consistencyCheck(getMethod(), schema, table.getName(),
                    (int) row, (int) range);
        }
        return id;
    }

    /**
     * A recursive method for divide & conquer consitency checking. Divides
     * ranges of rows.
     * 
     * @param table Table to check.
     * @param row Which row to start the check at.
     * @param range Must be a number in power of two! Otherwise, ranges will be
     *            lost and not checked.
     * @param slaveDbTungsten DBMS connection to the slave's Tungsten shema.
     * @param host
     * @throws Exception
     */
    private void drillDown(Table table, long row, int range,
            Database slaveDbTungsten, String host) throws Exception
    {
        // Have we reached requested granularity?
        if (range <= granularity)
        {
            printvln("");
            print("Inconsistency at row=" + row + " range=" + range + " in "
                    + host);
            if (printValues)
            {
                println(":");
                printCurrentValues(slaveDbTungsten, table, row, range);
                println("vs. master values:");
                printCurrentValues(masterDbUser, table, row, range);
            }
            else
                println("");
        }
        else
        {
            int mid = range / 2;

            if (logger.isDebugEnabled())
                logger.debug(String.format("%d+%d", row, mid));

            // Issue the check.
            int idA = consistencyCheck(table, row, mid);
            if (!didCheckPass(slaveDbTungsten, idA))
            {
                printv("x");
                drillDown(table, row, mid, slaveDbTungsten, host);
            }
            else
                printv(".");

            if (logger.isDebugEnabled())
                logger.debug(String.format("%d+%d", row + mid, mid));

            // Issue the check.
            int idB = consistencyCheck(table, row + mid, mid);
            if (!didCheckPass(slaveDbTungsten, idB))
            {
                printv("x");
                drillDown(table, row + mid, mid, slaveDbTungsten, host);
            }
            else
                printv(".");
        }
    }

    /**
     * Prints columns of a table. Marks primary key ones with an asterisk.
     */
    private void printColumns(Table table)
    {
        ArrayList<Column> columns = table.getAllColumns();
        ArrayList<Column> pkCols = null;
        if (table.getPrimaryKey() != null)
            pkCols = table.getPrimaryKey().getColumns();
        for (Column column : columns)
        {
            print(column.getName());
            if (pkCols != null && pkCols.contains(column))
                print("*");
            print("\t");
        }
        println("");
    }

    /**
     * Print current values of a range of rows from a particular table.
     * 
     * @param conn DBMS connection to use.
     * @param row Row where to start (PK with MD5PK, row number with the
     *            limit-based MD5 method).
     * @param range How many rows to print after the first one.
     * @throws SQLException
     */
    private void printCurrentValues(Database conn, Table table, long row,
            int range) throws SQLException
    {
        String query = null;

        if (methodPk)
        {
            Key pk = table.getPrimaryKey();
            if (pk.getColumns().size() < 1)
            {
                fatal(ConsistencyCheck.Method.MD5PK
                        + " method works on tables with primary keys only: "
                        + schema + "." + table.getName(), null);
            }
            else if (pk.getColumns().size() > 1)
            {
                fatal(ConsistencyCheck.Method.MD5PK
                        + " method doesn't support tables with composite primary keys: "
                        + schema + "." + table.getName(), null);
            }
            else
            {
                String pkName = pk.getColumns().get(0).getName();
                query = String.format(
                        "SELECT * FROM %s.%s WHERE %s >= %d AND %s < %d",
                        schema, table.getName(), pkName, row, pkName, row
                                + range);
            }
        }
        else
        {
            query = String.format("SELECT * FROM %s.%s LIMIT %d,%d", schema,
                    table.getName(), row, range);
        }

        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next())
            {
                for (int col = 1; col <= columnCount; col++)
                {
                    print(rs.getObject(col) + "\t");
                }
                println("");
            }
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
    }

    /**
     * Retrieve maximum or minimum value of a table's primary key. Table must
     * have a single-column numeric key for this to work correctly.
     */
    private long retrieveMaxMinPK(Database conn, Table table, boolean max)
            throws Exception
    {
        long value = -1;

        if (table.getPrimaryKey() == null)
            throw new Exception(table.getName() + " has no PK");
        else if (table.getPrimaryKey().getColumns().size() != 1)
            throw new Exception(table.getName()
                    + " PK is not a single-column one: "
                    + table.getPrimaryKey().getColumns() + " columns");

        String function = "MIN";
        if (max)
            function = "MAX";

        String query = String
                .format("SELECT %s(%s) FROM %s", function, table
                        .getPrimaryKey().getColumns().get(0).getName(),
                        table.getName());
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            if (rs.next())
            {
                value = rs.getLong(1);
            }
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return value;
    }

    /**
     * Retrieves row count of a table.
     */
    private long retrieveRowCount(Database conn, Table table)
            throws SQLException
    {
        return retrieveRowCount(conn, table, -1, -1);
    }

    /**
     * Retrieve count of rows of a particular table. If fromPk and tillPk are
     * defined, then rows are counted in-between those PK values.
     */
    private long retrieveRowCount(Database conn, Table table, long fromPk,
            long tillPk) throws SQLException
    {
        long count = -1;
        String countOn = "*";
        String where = "";

        Key pk = table.getPrimaryKey();
        if (pk != null)
        {
            if (pk.getColumns().size() == 1)
            {
                // Optimize count on only the PK when possible.
                String pkName = pk.getColumns().get(0).getName();
                countOn = pkName;
                // Limit the range if asked.
                if (fromPk >= 0 && tillPk >= 0)
                {
                    where = String.format("WHERE %s >= %d AND %s < %d", pkName,
                            fromPk, pkName, tillPk);
                }
            }
            else if (pk.getColumns().size() > 1 && fromPk >= 0 && tillPk >= 0)
            {
                // Can't calculate row count for multi-column PK table.
                return -1;
            }
        }

        String query = String.format("SELECT COUNT(%s) FROM %s %s", countOn,
                table.getName(), where);
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery(query);
            if (rs.next())
            {
                count = rs.getLong(1);
            }
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return count;
    }

    /**
     * Retrieve consistency check from specific DBMS and check whether it passed
     * or failed.
     * 
     * @return true, if check succeeded, false, if failed.
     */
    private boolean didCheckPass(Database conn, int id) throws SQLException,
            InterruptedException, ReplicatorException
    {
        String query = String.format(
                "SELECT %s,%s,%s,%s FROM %s.%s WHERE %s = %d",
                ConsistencyTable.thisCrcColumnName,
                ConsistencyTable.thisCntColumnName,
                ConsistencyTable.masterCrcColumnName,
                ConsistencyTable.masterCntColumnName, serviceSchema,
                ConsistencyTable.TABLE_NAME, ConsistencyTable.idColumnName, id);

        int timeLeft = checkTimeout * 1000;
        while (timeLeft > 0)
        {
            long startSleepMillis = System.currentTimeMillis();

            Statement st = null;
            ResultSet rs = null;
            try
            {
                st = conn.createStatement();
                rs = st.executeQuery(query);
                if (rs.next())
                {
                    String thisCrc = rs
                            .getString(ConsistencyTable.thisCrcColumnName);
                    String thisCnt = rs
                            .getString(ConsistencyTable.thisCntColumnName);
                    String masterCrc = rs
                            .getString(ConsistencyTable.masterCrcColumnName);
                    String masterCnt = rs
                            .getString(ConsistencyTable.masterCntColumnName);
                    if (thisCnt.compareTo(masterCnt) == 0
                            && thisCrc.compareTo(masterCrc) == 0)
                    {
                        return true;
                    }
                    else
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("          |              CRC                 | cnt");
                            logger.debug("this      | " + thisCrc + " | "
                                    + thisCnt);
                            logger.debug("master    | " + masterCrc + " | "
                                    + masterCnt);
                        }
                        return false;
                    }
                }
            }
            finally
            {
                if (rs != null)
                {
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                    }
                }
                if (st != null)
                {
                    try
                    {
                        st.close();
                    }
                    catch (SQLException e)
                    {
                    }
                }
            }

            // Blocking before trying again.
            // TODO: any way to remove the sleep and make delay more smart?
            Thread.sleep(50);
            long sleepMillis = System.currentTimeMillis() - startSleepMillis;
            timeLeft -= sleepMillis;
        }
        throw new ReplicatorException(
                String.format(
                        "Timeout (%ds) while waiting for consistency check to complete",
                        checkTimeout));
    }

    /**
     * In URL substitute the database name to avoid errors.
     */
    private String substituteDbName(String url, String db)
    {
        Properties jProps = new Properties();
        jProps.setProperty("URL", url);
        if (db != null)
            jProps.setProperty("DBNAME", db);
        TungstenProperties.substituteSystemValues(jProps);
        return jProps.getProperty("URL");
    }

    /**
     * Connects to the Replicator JMX and returns the MBean.
     */
    private OpenReplicatorManagerMBean getOpenReplicator(String rmiHost,
            String rmiPort, String service)
    {
        // TODO: what about authentication?
        try
        {
            JMXConnector connMaster = JmxManager.getRMIConnector(rmiHost,
                    Integer.valueOf(rmiPort),
                    ReplicatorConf.RMI_DEFAULT_SERVICE_NAME, null);
            OpenReplicatorManagerMBean replicator = (OpenReplicatorManagerMBean) JmxManager
                    .getMBeanProxy(connMaster, OpenReplicatorManager.class,
                            OpenReplicatorManagerMBean.class, service, false,
                            false);
            if (replicator.isAlive())
                println(String.format("Connected to %s:%s service %s", rmiHost,
                        rmiPort, service));
            return replicator;
        }
        catch (Exception e)
        {
            println(e.getMessage());
            return null;
        }
    }

    protected static void printHelp()
    {
        println("DataScan Utility");
        println("Syntax: datascan [conf1|conn1] [conf2|conn2] -db <db> [scan-spec] [-json]");
        println("Conf options:");
        println("  -conf[1|2] path     - Path to a static-<svc>.properties file to read JDBC");
        println("     OR                 connection address and credentials");
        println("  -service[1|2] name  - Name of a replication service instead of path to config");
        println("OR connection options:");
        println("  -user[1|2] user     - JDBC username");
        println("  -pass[1|2] secret   - JDBC password");
        println("  -url[1|2] jdbcUrl   - JDBC connection string (use single quotes to escape)");
        println("-db db                - Database to use (will substitute "
                + DDLScanCtrl.DBNAME_VAR + " in the URL, if needed)");
        println("scan-spec:");
        println("  -check direct|replicated");
        println("    direct            - Check directly on servers. Use when replication is broken or not setup");
        println("    replicated        - Check through replication. Use when replication is healthy");
        println("  -method pk|limit");
        println("    pk                - Searches based on table's primary key");
        println("    limit             - Searches by row position (use with a static table and no missing rows)");
        println("                        Default: pk");
        println("  [-tables regex]     - Tables to scan (ignore for scanning whole database/schema)");
        println("  [-from row]         - Scan part of table (use with a single table)");
        println("  [-till row]           Depending on the method chosen argument is PK value or row position");
        println("                        Default: scan the whole table");
        println("  [-chunk rows]       - Starting chunk size. This translates to an acceptable maximum lock size");
        println("                        Default: 8192");
        println("                        Use \"single\" for automatically using chunk size equal to row count");
        println("  [-chunk-pause s]    - How long to pause before the next chunk");
        println("                        Default: 0 - don't pause");
        println("  [-granularity rows] - When to stop? Use to adjust level of detail of algorithms");
        println("                        Default: 1 - drill down to a single row.");
        println("  [-parallel threads] - How much to parallelize");
        println("                        Default: 1 - no parallelization");
        println("  [-optimistic-lock]  - Don't lock, but afterwards check for related changes in the THL");
        println("  [-recently-changed] - Looks up THL for what has changed until the last time and checks");
        println("                        only those rows and tables");
        println("  [-thl]              - Try to find references of inconsistent rows in THL");
        println("  [-timeout s]        - Time to wait for a single consistency check call to return");
        println("                        Default: 30");
        println("  [-values]           - Query current DBMS values of inconsistent rows");
        println("[-json]               - Outputs results in JSON for easier parsing");
        println("[-verbose]            - Output real-time details of the action");
        println("-help                 - Print this help display");
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    protected static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    protected static void print(String msg)
    {
        System.out.print(msg);
    }

    /**
     * Print a message to stdout with trailing new line character.
     * 
     * @param msg
     */
    private void printvln(String msg)
    {
        if (verbose)
            println(msg);
    }

    /**
     * Print a message to stdout without trailing new line character.
     * 
     * @param msg
     */
    private void printv(String msg)
    {
        if (verbose)
            print(msg);
    }

    /**
     * Abort following a fatal error.
     * 
     * @param msg
     * @param t
     */
    protected static void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (t != null)
            t.printStackTrace();
        fail();
    }

    /**
     * Exit with a process failure code.
     */
    protected static void fail()
    {
        System.exit(1);
    }

    /**
     * Exit with a process success code.
     */
    protected static void succeed()
    {
        System.exit(0);
    }

    /**
     * Reads a character from stdin, blocks until it is not received.
     * 
     * @return true if use pressed `y`, false otherwise.
     */
    protected static boolean readYes() throws IOException
    {
        return (System.in.read() == 'y');
    }

    /**
     * Returns a value of a given Boolean object or false if the object is null.
     * 
     * @param bool Boolean object to check and return.
     * @return the value of a given Boolean object or false if the object is
     *         null.
     */
    protected static boolean getBoolOrFalse(Boolean bool)
    {
        if (bool != null)
            return bool;
        else
            return false;
    }
}