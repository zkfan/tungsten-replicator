/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.datasource;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.csv.CsvWriter;
import com.continuent.tungsten.common.csv.NullPolicy;
import com.continuent.tungsten.common.file.FileIOException;
import com.continuent.tungsten.common.file.FileIOUtils;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.common.file.HdfsFileIO;
import com.continuent.tungsten.common.file.JavaFileIO;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements a connection for HDFS with methods that mimic the 'hadoop fs'
 * command verbs.
 */
public class HdfsConnection implements UniversalConnection
{
    private static final Logger logger = Logger.getLogger(HdfsConnection.class);
    private final HdfsFileIO    hdfsFileIO;

    /**
     * Creates a new instance.
     */
    public HdfsConnection(HdfsFileIO fileIO)
    {
        this.hdfsFileIO = fileIO;
    }

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
        csv.setSeparator('\001');
        csv.setQuoted(false);
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

    /**
     * Creates a directory.
     * 
     * @param path Directory path to create.
     * @param ignoreErrors If true, ignore errors if directory exists.
     */
    public void mkdir(String path, boolean ignoreErrors)
            throws ReplicatorException
    {
        FilePath remote = new FilePath(path);
        try
        {
            if (ignoreErrors)
                hdfsFileIO.mkdirs(remote);
            else
                hdfsFileIO.mkdir(remote);
        }
        catch (FileIOException e)
        {
            throw new ReplicatorException(
                    "Unable to create directory: hdfs path=" + path
                            + " message=" + e.getMessage(), e);
        }
    }

    /**
     * Delete a file or directory.
     * 
     * @param path Directory path to remove.
     * @param recursive If true, delete recursively
     * @param ignoreErrors If true, ignore errors
     */
    public void rm(String path, boolean recursive, boolean ignoreErrors)
            throws ReplicatorException
    {
        FilePath remote = new FilePath(path);
        try
        {
            hdfsFileIO.delete(remote, recursive);
        }
        catch (FileIOException e)
        {
            if (ignoreErrors)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Ignoring delete error: path=" + path, e);
                }
            }
            else
            {
                throw new ReplicatorException(
                        "Unable to delete file or directory: hdfs path=" + path
                                + " recursive=" + recursive + " message="
                                + e.getMessage(), e);
            }
        }
    }

    /**
     * Move a local file to HDFS.
     * 
     * @param localPath Path of input file on local file system
     * @param hdfsPath Path of file in HDFS
     */
    public void put(String localPath, String hdfsPath)
            throws ReplicatorException
    {
        JavaFileIO localFileIO = new JavaFileIO();
        FilePath local = new FilePath(localPath);
        FilePath remote = new FilePath(hdfsPath);
        try
        {
            FileIOUtils.copyBytes(localFileIO.getInputStream(local),
                    hdfsFileIO.getOutputStream(remote), 1024, true);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to copy file: local path="
                    + localPath + " hdfs path=" + hdfsPath + " message="
                    + e.getMessage(), e);
        }
    }
}