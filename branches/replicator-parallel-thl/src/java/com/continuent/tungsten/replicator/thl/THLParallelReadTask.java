/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.thl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.parallel.Partitioner;
import com.continuent.tungsten.replicator.storage.parallel.PartitionerResponse;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.log.LogEventReadFilter;
import com.continuent.tungsten.replicator.thl.log.LogEventReplReader;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Performs coordinated reads on the THL on behalf of a particular client (a
 * task thread) and buffers log records up to a local limit.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelReadTask implements Runnable
{
    private static Logger                         logger                  = Logger.getLogger(THLParallelReadTask.class);

    // Task number on whose behalf we are reading.
    private final int                             taskId;

    // Partitioner instance.
    private final Partitioner                     partitioner;

    // Counters to coordinate queue operation.
    private AtomicCounter                         headSeqnoCounter;
    private AtomicLong                            highWaterMark           = new AtomicLong(
                                                                                  0);
    private AtomicLong                            discardCount            = new AtomicLong(
                                                                                  0);

    // Ordered queue of events for reading and current seqno for reading.
    private final BlockingQueue<ReplEvent>        eventQueue;
    private long                                  readSeqno               = 0;
    private boolean                               inFragmentedTransaction = false;

    // Pending control events to be integrated into the event queue and seqno
    // of next event if known.
    private final BlockingQueue<ReplControlEvent> controlQueue;
    private long                                  nextControlSeqno        = Long.MAX_VALUE;

    // Connection to the log.
    private THL                                   thl;
    private LogConnection                         connection;

    // Throwable trapped from run loop.
    private Throwable                             throwable;

    // Thread ID for this read task.
    private volatile Thread                       taskThread;

    // Flag indicating task is cancelled.
    private volatile boolean                      cancelled               = false;

    /**
     * Instantiate a read task.
     */
    public THLParallelReadTask(int taskId, THL thl, Partitioner partitioner,
            AtomicCounter headSeqnoCounter, int maxSize)
    {
        this.taskId = taskId;
        this.thl = thl;
        this.partitioner = partitioner;
        this.headSeqnoCounter = headSeqnoCounter;
        this.eventQueue = new LinkedBlockingQueue<ReplEvent>(maxSize);
        this.controlQueue = new LinkedBlockingQueue<ReplControlEvent>(maxSize);
    }

    /**
     * Set the starting sequence number for reads. Must be called before
     * prepare().
     */
    public synchronized void setSeqno(long seqno)
    {
        this.readSeqno = seqno;
    }

    /**
     * Connect to THL and seek start sequence number. Must be called before
     * run().
     */
    public synchronized void prepare(PluginContext context)
            throws THLException, InterruptedException
    {
        // Connect to the log and seek to the current record.
        connection = thl.connect(true);
        if (!connection.seek(readSeqno))
        {
            throw new THLException(
                    "Unable to locate starting seqno in log: seqno="
                            + readSeqno + " store=" + thl.getName()
                            + " taskId=" + taskId);
        }

        // Add a read filter that will accept only events that are in this
        // partition.
        LogEventReadFilter filter = new LogEventReadFilter()
        {
            public boolean accept(LogEventReplReader reader)
                    throws THLException
            {
                ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                        reader.getSeqno(), reader.getFragno(),
                        reader.isLastFrag(), reader.getSourceId(),
                        reader.getEpochNumber(), reader.getEventId(),
                        reader.getShardId());
                PartitionerResponse response;
                try
                {
                    response = partitioner.partition(header, taskId);
                }
                catch (THLException e)
                {
                    throw e;
                }
                catch (ReplicatorException e)
                {
                    throw new THLException(e.getMessage(), e);
                }
                return (taskId == response.getPartition());
            }
        };
        connection.setReadFilter(filter);
    }

    /**
     * Start the task thread. This must be called after prepare.
     */
    public synchronized void start()
    {
        if (this.taskThread == null)
        {
            taskThread = new Thread(this);
            taskThread.setName("store-" + thl.getName() + "-" + taskId);
            taskThread.start();
        }
    }

    /**
     * Cancel the thread. This must be called prior to release.
     */
    public synchronized void stop()
    {
        cancelled = true;
        if (this.taskThread != null)
        {
            taskThread.interrupt();
            try
            {
                taskThread.join(2000);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Terminate reader task and free all resources. Must be called following
     * run().
     */
    public synchronized void release()
    {
        if (connection != null)
        {
            connection.release();
            connection = null;
            controlQueue.clear();
            eventQueue.clear();
        }
    }

    /**
     * Implements read loop on the log to feed event queue.
     */
    @Override
    public void run()
    {
        // Connect to store.
        try
        {
            while (!cancelled)
            {
                // Check for a new control event.
                if (nextControlSeqno == Long.MAX_VALUE)
                {
                    ReplEvent controlEvent = controlQueue.peek();
                    if (controlEvent != null)
                    {
                        nextControlSeqno = controlEvent.getSeqno();
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Detected pending control event:  taskId="
                                    + taskId + " seqno=" + nextControlSeqno);
                        }
                    }
                }

                // If there is a pending control event, we know we want to
                // grab that, provided we are not in a fragmented transaction.
                // Otherwise try to get an event from the log.
                if (nextControlSeqno <= readSeqno
                        && !this.inFragmentedTransaction)
                {
                    // We know we want this event, so just grab it.
                    ReplControlEvent replEvent = controlQueue.take();
                    readSeqno = replEvent.getSeqno();

                    // Add to queue.
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Adding control event to parallel queue:  taskId="
                                + taskId
                                + " seqno="
                                + nextControlSeqno
                                + " type=" + replEvent.getEventType());
                    }
                    eventQueue.put(replEvent);

                    // Reset pending control event.
                    nextControlSeqno = Long.MAX_VALUE;
                }
                else
                {
                    // If we don't have a control event, read the next event
                    // from the log.
                    THLEvent thlEvent = connection.next();
                    readSeqno = thlEvent.getSeqno();
                    inFragmentedTransaction = !thlEvent.getLastFrag();
                    highWaterMark.set(readSeqno);

                    // If we do not want it, just go to the next event. This
                    // would be null if the read filter discarded the event due
                    // to it being in another partition.
                    ReplDBMSEvent replDBMSEvent = (ReplDBMSEvent) thlEvent
                            .getReplEvent();
                    if (replDBMSEvent == null)
                    {
                        discardCount.incrementAndGet();
                        continue;
                    }

                    // Discard empty events. These should not be common.
                    DBMSEvent dbmsEvent = replDBMSEvent.getDBMSEvent();
                    if (dbmsEvent == null | dbmsEvent instanceof DBMSEmptyEvent
                            || dbmsEvent.getData().size() == 0)
                    {
                        discardCount.incrementAndGet();
                        continue;
                    }

                    // Ensure it is safe to process this value.
                    headSeqnoCounter.waitSeqnoGreaterEqual(replDBMSEvent
                            .getSeqno());

                    // Add to queue.
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Adding event to parallel queue:  taskId="
                                + taskId + " seqno=" + replDBMSEvent.getSeqno()
                                + " fragno=" + replDBMSEvent.getFragno());
                    }
                    eventQueue.put(replDBMSEvent);
                }
            }
        }
        catch (InterruptedException e)
        {
            if (!cancelled)
                logger.warn("Unexpected interrupt before reader thread was cancelled");
        }
        catch (Throwable e)
        {
            logger.error("Read failed on transaction log: seqno=" + readSeqno
                    + " taskId=" + taskId, e);
            throwable = e;
        }

        // Close up shop.
        logger.info("Terminating parallel reader thread: seqno=" + readSeqno
                + " store=" + thl.getName() + " taskId=" + taskId);
    }

    // QUEUE INTERFACE STARTS HERE.

    /**
     * Returns the current queue size.
     */
    public int size()
    {
        return eventQueue.size();
    }

    /**
     * Removes and returns next event from the queue, blocking if empty. This
     * call blocks if no event is available.
     * 
     * @return The next event in the queue
     * @throws InterruptedException Thrown if method is interrupted
     * @throws ReplicatorException Thrown if the reader thread has failed
     */
    public ReplEvent get() throws InterruptedException, ReplicatorException
    {
        // Check for a failure.
        if (throwable != null)
        {
            throw new ReplicatorException("THL reader thread failed", throwable);
        }

        // Get the event and return it.
        ReplEvent event = eventQueue.take();
        if (logger.isDebugEnabled())
        {
            logger.debug("Returning event from queue: seqno="
                    + event.getSeqno() + " type="
                    + event.getClass().getSimpleName() + " taskId=" + taskId
                    + " activeSize=" + size());
        }
        return event;
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplEvent peek() throws InterruptedException
    {
        return eventQueue.peek();
    }

    // Inserts a control event in local queue.
    public void putControlEvent(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        synchronized (eventQueue)
        {
            // See if we need to put the control event directly in the event
            // queue.
            controlQueue.put(controlEvent);
        }
    }
}