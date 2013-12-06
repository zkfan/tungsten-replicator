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

package com.continuent.tungsten.replicator.catalog;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;

/**
 * Denotes an implementation of the replicator catalog data, which consists of a
 * set of "tables" that hold metadata used to control replication. Catalogs may
 * implement such tables using relational tables, files, or any other suitable
 * means.
 * <p/>
 * All data required for operation must be provided through property setters.
 * Catalogs do not implement the ReplicatorPlugin lifecycle or access the
 * PluginContext implementation as this introduces dependencies that prevent
 * easy testing and hurt portability between DBMS store types.
 * 
 * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin
 * @see com.continuent.tungsten.replicator.plugin.PluginContext
 */
public interface Catalog extends CatalogEntity
{
    /**
     * Set the name of the replicator service that is using this catalog.
     */
    public void setServiceName(String serviceName);

    /**
     * Return the name of the replicator service that is using this catalog.
     */
    public String getServiceName();

    /**
     * Set the number of channels to track. This is the basic mechanism to
     * support parallel replication.
     */
    public void setChannels(int channels);

    /**
     * Return the number of channels to track.
     */
    public int getChannels();

    /**
     * Returns a ready-to-use CommitSeqno instance for operations on commit
     * seqno data.
     */
    public CommitSeqno getCommitSeqno();

    /**
     * Returns a ready-to-use wrapped connection for operations on a database.
     */
    public Database getConnection() throws ReplicatorException;

    /**
     * Releases a wrapped connection.
     */
    public void releaseConnection(Database conn);
}