/**
 * Tungsten: An Application Server for uni/cluster.
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

import java.sql.Timestamp;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Runs tests on the catalog manager to ensure we can add, find, and remove
 * catalogs.
 */
public class AbstractCatalogTest
{
    private static Logger        logger         = Logger.getLogger(AbstractCatalogTest.class);

    // Properties for catalog test.
    protected TungstenProperties catalogProps;
    protected String             catalogClass;
    protected CatalogManager     catalogManager = new CatalogManager();

    /**
     * Verify that after initialization the catalog contents are available.
     */
    @Test
    public void testInitialization() throws Exception
    {
        // Create a separate catalog for this test.
        catalogProps.setString("serviceName", "test_initialization");
        catalogManager.add("testInitialization", catalogClass, catalogProps);

        // Get the catalog and ensure tables are cleared.
        Catalog c = catalogManager.find("testInitialization");
        c.clear();

        // Now initialize the tables.
        c.initialize();

        // Verify that we can find commit seqno data.
        CommitSeqno commitSeqno = c.getCommitSeqno();
        Assert.assertEquals("Looking for initialized commit seqno", -1,
                commitSeqno.minCommitSeqno().getSeqno());
    }

    /**
     * Verify that if we initialize a catalog we can update the commit seqno
     * position and read the updated value back.
     */
    @Test
    public void testSeqno() throws Exception
    {
        Catalog c = prepareCatalog("testSeqno");

        // Retrieve the initial data.
        Database conn = c.getConnection();
        CommitSeqnoAccessor accessor = c.getCommitSeqno().createAccessor(0,
                conn);
        ReplDBMSHeader initial = accessor.lastCommitSeqno();
        Assert.assertNotNull("Expect non-null initial header", initial);
        Assert.assertEquals("Expected initial seqno", -1, initial.getSeqno());

        // Change the seqno and update.
        ReplDBMSHeaderData newHeader = new ReplDBMSHeaderData(4, (short) 2,
                true, "foo", 1, "someEvent#", "someShard", new Timestamp(
                        10000000), 25);
        accessor.updateLastCommitSeqno(newHeader, 30);

        // Retrieve the header and ensure values match.
        ReplDBMSHeader retrieved = accessor.lastCommitSeqno();
        Assert.assertEquals("Checking seqno", 4, retrieved.getSeqno());
        Assert.assertEquals("Checking fragno", 2, retrieved.getFragno());
        Assert.assertEquals("Checking lastFrag", true, retrieved.getLastFrag());
        Assert.assertEquals("Checking sourceId", "foo", retrieved.getSourceId());
        Assert.assertEquals("Checking epochNumber", 1,
                retrieved.getEpochNumber());
        Assert.assertEquals("Checking event ID", "someEvent#",
                retrieved.getEventId());
        Assert.assertEquals("Checking shard ID", "someShard",
                retrieved.getShardId());
        Assert.assertEquals("Checking extractedTstamp",
                new Timestamp(10000000), retrieved.getExtractedTstamp());
        Assert.assertEquals("Checking appliedLatency", 30,
                retrieved.getAppliedLatency());

        // Release resources and exit.
        c.releaseConnection(conn);
    }

    /**
     * Verify that we can allocate many accessors in succession to read and
     * update the commit seqno position.
     */
    @Test
    public void testSeqnoManyAccessors() throws Exception
    {
        Catalog c = prepareCatalog("testSeqnoManyAccessors");
        CommitSeqno commitSeqno = c.getCommitSeqno();

        // Loop through many times.  
        // TODO:  Raise # to 10000. 
        for (int i = 0; i < 100; i++)
        {
            if (i > 0 && (i % 1000) == 0)
                logger.info("Iteration: " + i);
            Database conn = c.getConnection();
            CommitSeqnoAccessor accessor = commitSeqno.createAccessor(0, conn);

            // Check the last position updated.
            ReplDBMSHeader lastHeader = accessor.lastCommitSeqno();
            Assert.assertEquals("Checking seqno", i - 1, lastHeader.getSeqno());

            // Update the header to the current position.
            ReplDBMSHeaderData newHeader = new ReplDBMSHeaderData(i, (short) 2,
                    true, "foo", 1, "someEvent#", "someShard", new Timestamp(
                            10000000), 25);
            accessor.updateLastCommitSeqno(newHeader, 25);

            // Discard the accessor and connection.
            accessor.close();
            c.releaseConnection(conn);
        }
    }

    /**
     * Verify that the seqno is correctly stored and returned for each allocated
     * channel.
     */
    @Test
    public void testSeqnoChannels() throws Exception
    {
        Catalog c = prepareCatalog("testSeqnoChannels");
        int channels = c.getChannels();
        Database conn = c.getConnection();
        CommitSeqno commitSeqno = c.getCommitSeqno();
        CommitSeqnoAccessor[] accessors = new CommitSeqnoAccessor[channels];

        // Expand the commit sequence numbers out to the full number of
        // channels.
        commitSeqno.expandTasks();

        // Allocate accessor and update for each channel.
        for (int i = 0; i < channels; i++)
        {
            accessors[i] = commitSeqno.createAccessor(i, conn);
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(i * 2,
                    (short) 0, true, "foo", 1, "someEvent#", "someShard",
                    new Timestamp(10000000), 25);
            accessors[i].updateLastCommitSeqno(header, 25);
        }

        // Read back stored header and deallocate accessor for each channel.
        for (int i = 0; i < channels; i++)
        {
            ReplDBMSHeader retrieved = accessors[i].lastCommitSeqno();
            Assert.assertEquals("Checking seqno: channel=" + i, i * 2,
                    retrieved.getSeqno());
            accessors[i].close();
        }

        // Release resources and exit.
        c.releaseConnection(conn);
    }

    /**
     * Verify that seqno values are persistent even if we allocate the catalog a
     * second time.
     */
    @Test
    public void testSeqnoPersistence() throws Exception
    {
        Catalog c = prepareCatalog("testSeqnoPersistence");
        int channels = c.getChannels();

        // Expand the commit sequence numbers out to the full number of
        // channels.
        CommitSeqno commitSeqno = c.getCommitSeqno();
        commitSeqno.expandTasks();

        // Allocate accessor and update for each channel.
        Database conn = c.getConnection();
        for (int i = 0; i < channels; i++)
        {
            CommitSeqnoAccessor accessor = commitSeqno.createAccessor(i, conn);
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(i * 2,
                    (short) 0, true, "foo", 1, "someEvent#", "someShard",
                    new Timestamp(10000000), 25);
            accessor.updateLastCommitSeqno(header, 25);
            accessor.close();
        }
        commitSeqno.release();

        // Close the catalog and add a new one.
        c.release();
        catalogManager.remove("testSeqnoPersistence");
        catalogManager.add("testSeqnoPersistence", catalogClass, catalogProps);
        Catalog c2 = catalogManager.find("testSeqnoPersistence");

        // Read back stored header and deallocate accessor for each channel.
        Database conn2 = c2.getConnection();
        CommitSeqno commitSeqno2 = c2.getCommitSeqno();
        for (int i = 0; i < channels; i++)
        {
            CommitSeqnoAccessor accessor = commitSeqno2
                    .createAccessor(i, conn2);
            ReplDBMSHeader retrieved = accessor.lastCommitSeqno();
            Assert.assertEquals("Checking seqno: channel=" + i, i * 2,
                    retrieved.getSeqno());
            accessor.close();
        }
        commitSeqno.release();

        // Release resources and exit.
        c2.releaseConnection(conn);
    }

    /**
     * Prepares a catalog and returns same to caller.
     */
    private Catalog prepareCatalog(String name) throws ReplicatorException,
            InterruptedException
    {
        catalogProps.setString("serviceName", name);
        catalogManager.add(name, catalogClass, catalogProps);

        // Get the catalog and ensure tables are cleared.
        Catalog c = catalogManager.find(name);
        c.clear();
        c.initialize();

        return c;
    }
}