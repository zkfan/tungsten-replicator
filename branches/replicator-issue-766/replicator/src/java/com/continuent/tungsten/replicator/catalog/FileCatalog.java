/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011-2013 Continuent Inc.
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

package com.continuent.tungsten.replicator.catalog;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.file.FileIO;
import com.continuent.tungsten.common.file.FilePath;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;

/**
 * Implements a catalog that uses a file system to store catalog information.
 */
public class FileCatalog implements Catalog
{
    private static Logger logger   = Logger.getLogger(FileCatalog.class);

    // Properties.
    private String        serviceName;
    private int           channels = 1;
    private String        directory;

    // Catalog tables.
    FileCommitSeqno       commitSeqno;

    // File IO-related variables.
    FilePath              rootDir;
    FilePath              serviceDir;

    /** Create new instance. */
    public FileCatalog()
    {
    }

    public String getDirectory()
    {
        return directory;
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }

    // CATALOG API

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#setServiceName(java.lang.String)
     */
    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getServiceName()
     */
    public String getServiceName()
    {
        return serviceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#setChannels(int)
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getChannels()
     */
    public int getChannels()
    {
        return channels;
    }

    /**
     * Instantiate and configure all catalog tables.
     */
    @Override
    public void configure() throws ReplicatorException, InterruptedException
    {
        // Configure file paths.
        rootDir = new FilePath(directory);
        serviceDir = new FilePath(rootDir, serviceName);

        // Configure tables.
        commitSeqno = new FileCommitSeqno();
        commitSeqno.setServiceName(serviceName);
        commitSeqno.setChannels(channels);
        commitSeqno.setServiceDir(serviceDir);
    }

    /**
     * Prepare all catalog tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        // Ensure the service directory is ready for use.
        FileIO fileIO = new FileIO();
        if (!fileIO.exists(serviceDir))
        {
            logger.info("Service directory does not exist, creating: "
                    + serviceDir.toString());
            fileIO.mkdirs(serviceDir);
        }

        // Ensure everything exists now.
        if (!fileIO.readable(serviceDir))
        {
            throw new ReplicatorException(
                    "Service directory does not exist or is not readable: "
                            + serviceDir.toString());
        }
        else if (!fileIO.writable(serviceDir))
        {
            throw new ReplicatorException("Service directory is not writable: "
                    + serviceDir.toString());
        }

        // Prepare all tables.
        commitSeqno.prepare();
    }

    /**
     * Release all catalog tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        commitSeqno.release();
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        logger.info("Initializing catalog files: service=" + serviceName
                + " directory=" + directory);
        commitSeqno.initialize();
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        commitSeqno.clear();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        return commitSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getConnection()
     */
    public Database getConnection() throws ReplicatorException
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#releaseConnection(com.continuent.tungsten.replicator.database.Database)
     */
    public void releaseConnection(Database conn)
    {
    }
}