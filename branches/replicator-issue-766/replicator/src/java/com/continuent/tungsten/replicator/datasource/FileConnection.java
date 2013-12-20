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
 * Initial developer(s): Scott Martin
 * Contributor(s): Robert Hodges, Stephane Giron, Linas Virbalas
 */

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedWriter;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.csv.NullPolicy;

/**
 * Implements a dummy connection for use with data sources that do not have
 * connections.
 */
public class FileConnection implements UniversalConnection
{
    /**
     * Returns a properly configured CsvWriter to generate CSV according to the
     * preferred conventions of this data source type.
     * 
     * @param writer A buffered writer to receive CSV output
     * @return A property configured CsvWriter instance
     */
    public CsvWriter getCsvWriter(BufferedWriter writer)
    {
        CsvWriter csv = new CsvWriter(writer);
        csv.setQuoteChar('"');
        csv.setQuoted(true);
        csv.setEscapeChar('\\');
        csv.setEscapedChars("\\");
        csv.setNullPolicy(NullPolicy.nullValue);
        csv.setNullValue("\\N");
        csv.setWriteHeaders(false);
        return csv;
    }

    /**
     * Commit the current transaction, which means to make a best effort to
     * ensure any data written to the connection are durable.
     * 
     * @throws Exception Thrown if the operation fails
     */
    public void commit() throws Exception
    {
        // Do nothing.
    }

    /**
     * Roll back the current transaction, which means to make a best effort to
     * ensure any data written to the connection since the last commit are
     * cleaned up.
     * 
     * @throws Exception Thrown if the operation fails
     */
    public void rollback() throws Exception
    {
        // Do nothing.
    }

    /**
     * Sets the commit semantics operations on the connection.
     * 
     * @param autoCommit If true each operation commits automatically; if false
     *            any further operations are enclosed in a transaction
     * @throws Exception Thrown if the operation fails
     */
    public void setAutoCommit(boolean autoCommit) throws Exception
    {
        // Do nothing.
    }

    /**
     * Closes the connection and releases resource.
     */
    public void close()
    {
        // Do nothing.
    }
}