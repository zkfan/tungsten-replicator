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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;

/**
 * Implements a dummy catalog type for testing.
 */
public class SampleCatalog implements Catalog
{
    private static Logger logger   = Logger.getLogger(SampleCatalog.class);

    // Properties.
    private String        serviceName;
    private int           channels = 1;
    private String        myParameter;

    /** Create new instance. */
    public SampleCatalog()
    {
    }

    public String getMyParameter()
    {
        return myParameter;
    }

    public void setMyParameter(String myParameter)
    {
        this.myParameter = myParameter;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public int getChannels()
    {
        return channels;
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
     * @see com.continuent.tungsten.replicator.catalog.Catalog#setChannels(int)
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * Instantiate and configure all catalog tables.
     */
    @Override
    public void configure() throws ReplicatorException, InterruptedException
    {
        logger.info("Configuring catalog: service=" + serviceName);
    }

    /**
     * Prepare all catalog tables for use.
     */
    @Override
    public void prepare() throws ReplicatorException, InterruptedException
    {
        logger.info("Preparing catalog: service=" + serviceName);
    }

    /**
     * Release all catalog tables.
     */
    @Override
    public void release() throws ReplicatorException, InterruptedException
    {
        logger.info("Releasing catalog: service=" + serviceName);
    }

    /**
     * Ensure all tables are ready for use, creating them if necessary.
     */
    @Override
    public void initialize() throws ReplicatorException, InterruptedException
    {
        logger.info("Initializing catalog tables: service=" + serviceName);
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        logger.info("Clearing catalog tables: service=" + serviceName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getCommitSeqno()
     */
    @Override
    public CommitSeqno getCommitSeqno()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#getConnection()
     */
    public Database getConnection() throws ReplicatorException
    {
        // Not implemented for now.
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.Catalog#releaseConnection(com.continuent.tungsten.replicator.database.Database)
     */
    public void releaseConnection(Database conn)
    {
        // Not implemented for now.
    }
}