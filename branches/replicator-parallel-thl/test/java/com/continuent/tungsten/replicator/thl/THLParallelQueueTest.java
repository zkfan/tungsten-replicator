/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.EventDispatcher;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * Implements a test of parallel THL operations. Parallel THL operation requires
 * a pipeline THL coupled with a THLParallelQueue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueTest extends TestCase
{
    private static Logger logger = Logger.getLogger(THLParallelQueueTest.class);

    /*
     * Verify that we can start and stop a pipeline containing a THL with a
     * THLParallelQueue.
     */
    public void testPipelineStartStop() throws Exception
    {
        logger.info("##### testPipelineStartStop #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testPipelineStartStop", 1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /*
     * Verify that a pipeline with a single channel successfully transmits
     * events from end to end. 
     */
    public void testSingleChannelOperation() throws Exception
    {
        logger.info("##### testSingleChannelOperation #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testSingleChannelOperation", 1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSEvent> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSEvent lastEvent = wait.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 server events", 9, lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0, thl.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9, thl.getMaxStoredSeqno());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    // Generate configuration properties for a three stage-pipeline
    // that loads events into a THL then loadsa parallel queue.
    public TungstenProperties generateTHLParallelQueueProps(String schemaName,
            int channels) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract,feed,apply", "thl,thl-queue");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("feed", "thl-extract", "thl-queue-apply", null);
        builder.addStage("apply", "thl-queue-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "tlh-queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags", "1");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Feed stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply", THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName", "thl-queue");

        // Apply stage components.
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName", "thl-queue");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate configuration properties for a double stage-pipeline
    // going through THL. This pipeline uses a queue as the initial head
    // of the queue.
    public TungstenProperties generateTHLParallelQueueProps(String schemaName)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract, apply", "queue,thl");
        builder.addStage("extract", "queue", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "queue", InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "queue", "storeName", "queue");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName)
    {
        File logDir = new File(logDirName);
        // Delete old log if present.
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // Create new log directory.
        logDir.mkdirs();
        return logDir;
    }

    // Returns a well-formed event with a default shard ID.
    private ReplDBMSEvent createEvent(long seqno)
    {
        return createEvent(seqno, ReplOptionParams.SHARD_ID_UNKNOWN);
    }

    // Returns a well-formed ReplDBMSEvent with a specified shard ID.
    private ReplDBMSEvent createEvent(long seqno, String shardId)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, dbmsEvent);
        replDbmsEvent.getDBMSEvent().addMetadataOption(
                ReplOptionParams.SHARD_ID, shardId);
        return replDbmsEvent;
    }
}