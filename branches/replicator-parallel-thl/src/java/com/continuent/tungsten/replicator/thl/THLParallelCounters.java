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

import java.util.concurrent.atomic.AtomicLong;

import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a set of counters to help maintain the illusion that the parallel
 * queue is a single in-memory queue. This includes the following operations:
 * <ul>
 * <li>Block entries to the queue at a particular sequence number</li>
 * <li>Wait until the queue is empty</li>
 * </ul>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelCounters
{
    /** Synchronized counter on head of queue. */
    private final AtomicCounter headSeqno = new AtomicCounter(0); 

    /** Number of events discarded. */
    private final AtomicLong discardCount = new AtomicLong(0);

    /**
     * Records the low water mark for each parallel queue, i.e., the tail
     * sequence number.
     */
    private final long[]     lowWaterMark;

    /**
     * Records the high water mark for each parallel queue, i.e., the head
     * sequence number.
     */
    private final long[]     highWaterMark;

    /** Instantiate new counters. */
    public THLParallelCounters(int size)
    {
        lowWaterMark = new long[size];
        highWaterMark = new long[size];
    }
    
    /** Fetch the head sequence number counter. */
    public AtomicCounter getHeadSeqno()
    {
        return headSeqno;
    }

    /** Fetch the discard count. */
    public AtomicLong getDiscardCount()
    {
        return discardCount;
    }
    
    /**
     * Records the low water mark for a parallel queue.
     * 
     * @param taskId Task ID of the queue
     * @param seqno Sequence number
     */
    public synchronized void setLowWaterMark(int taskId, long seqno)
    {
        lowWaterMark[taskId] = seqno;
    }

    /**
     * Returns the minimum low water mark across all queues.
     */
    public synchronized long getLowWaterMark()
    {
        long seqno = Long.MAX_VALUE;
        for (int i = 0; i < lowWaterMark.length; i++)
        {
            seqno = Math.min(seqno, lowWaterMark[i]);
        }
        return seqno;
    }

    /**
     * Records the high water mark for a parallel queue.
     * 
     * @param taskId Task ID of the queue
     * @param seqno Sequence number
     */
    public synchronized void setHighWaterMark(int taskId, long seqno)
    {
        highWaterMark[taskId] = seqno;
    }

    /**
     * Returns the maximum high water mark across all queues.
     */
    public synchronized long getHighWaterMark()
    {
        long seqno = 0;
        for (int i = 0; i < highWaterMark.length; i++)
        {
            seqno = Math.max(seqno, highWaterMark[i]);
        }
        return seqno;
    }
}