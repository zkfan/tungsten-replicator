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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.catalog;

import java.sql.SQLException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * Adapts the current CommitSeqnoTable class to the new CommitSeqnoData
 * interface.
 */
public class SqlCommitSeqnoTableAdapter implements CommitSeqno
{
    // Properties.
    private int              channels                = -1;
    private Database         conn                    = null;
    private boolean          syncNativeSlaveRequired = false;

    // Instances to handle commit seqno operations.
    private CommitSeqnoTable commitSeqnoTable;
    private boolean          prepared                = false;

    /** Create a new instance. */
    public SqlCommitSeqnoTableAdapter()
    {
    }

    public void setSyncNativeSlaveRequired(boolean syncNativeSlaveRequired)
    {
        this.syncNativeSlaveRequired = syncNativeSlaveRequired;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqnoData#setChannels(int)
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CatalogTable#configure()
     */
    public void configure() throws ReplicatorException, InterruptedException
    {
        // Check channels.
        if (channels < 0)
        {
            throw new ReplicatorException(
                    "Channels are not set for commit seqno");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CatalogTable#prepare()
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        try
        {
            commitSeqnoTable = new CommitSeqnoTable(conn, null, null,
                    syncNativeSlaveRequired);
            commitSeqnoTable.prepare(0);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to prepare commit seqno table: " + e.getMessage(),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CatalogTable#release()
     */
    public void release() throws ReplicatorException, InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqnoData#initialize()
     */
    public void initialize() throws ReplicatorException
    {
        try
        {
            commitSeqnoTable.initializeTable(channels);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to initialize commit seqno table: "
                            + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqnoData#expandTasks()
     */
    public void expandTasks() throws ReplicatorException
    {
        try
        {
            commitSeqnoTable.expandTasks(channels);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unabled to expand tasks: "
                    + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqnoData#reduceTasks()
     */
    public boolean reduceTasks() throws ReplicatorException
    {
        try
        {
            return commitSeqnoTable.reduceTasks(conn, channels);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unabled to reduce tasks: "
                    + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqnoData#minCommitSeqno()
     */
    public ReplDBMSHeader minCommitSeqno() throws ReplicatorException
    {
        try
        {
            return commitSeqnoTable.minCommitSeqno();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unabled to determine minimum commit seqno: "
                            + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.catalog.CommitSeqno#createAccessor(int,
     *      com.continuent.tungsten.replicator.database.Database)
     */
    public CommitSeqnoAccessor createAccessor(int taskId, Database conn)
    {
        return null;
    }

    /**
     * Updates the last committed seqno for a single channel. This is a client
     * call used by appliers to mark the restart position.
     */
    public void updateLastCommitSeqno(int taskId, ReplDBMSHeader header,
            long appliedLatency) throws ReplicatorException
    {
        try
        {
            ensurePrepared(taskId);
            commitSeqnoTable.updateLastCommitSeqno(taskId, header,
                    appliedLatency);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to update last commit seqno: " + e.getMessage(), e);
        }
    }

    /**
     * Fetches header data for last committed transaction for a particular
     * channel. This is a client call to get the restart position.
     */
    public ReplDBMSHeader lastCommitSeqno(int taskId)
            throws ReplicatorException
    {
        try
        {
            ensurePrepared(taskId);
            return commitSeqnoTable.lastCommitSeqno(taskId);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to retrieve last commit seqno: " + e.getMessage(),
                    e);
        }
    }

    // Ensure that we have prepared the connection for task-related operations.
    private void ensurePrepared(int taskId) throws ReplicatorException,
            SQLException
    {
        if (!prepared)
        {
            commitSeqnoTable.prepare(taskId);
            prepared = true;
        }
    }

    @Override
    public void clear() throws ReplicatorException, InterruptedException
    {
        // TODO Auto-generated method stub

    }
}