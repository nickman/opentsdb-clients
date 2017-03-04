// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.client.buffer;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * <p>Title: BufferArenaMonitor</p>
 * <p>Description: A netty pooled byte buffer allocation monitor</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.buffer.BufferArenaMonitor</code></p>
 * FIXME:  !! the reset to zero will hand out bad stats. Need to put all stats
 * in a long array stored in an atomic ref.
 */
public class BufferArenaMonitor implements BufferArenaMonitorMBean, Runnable {
	/** A scheduler to schedule monitor stats collections */
	protected static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory(){
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "BufferArenaMonitorThread#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	
	/** The number of allocations */
	protected long allocations = -1;
	/** The number of tiny allocations */
	protected long tinyAllocations = -1;
	/** The number of small allocations */
	protected long smallAllocations = -1;
	/** The number of normal allocations */
	protected long normalAllocations = -1;
	/** The number of huge allocations */
	protected long hugeAllocations = -1;
	/** The number of deallocations */
	protected long deallocations = -1;
	/** The number of tiny deallocations */
	protected long tinyDeallocations = -1;
	/** The number of small deallocations */
	protected long smallDeallocations = -1;
	/** The number of normal deallocations */
	protected long normalDeallocations = -1;
	/** The number of huge deallocations */
	protected long hugeDeallocations = -1;
	/** The number of active allocations */
	protected long activeAllocations = -1;
	/** The number of active tiny allocations */
	protected long activeTinyAllocations = -1;
	/** The number of active small allocations */
	protected long activeSmallAllocations = -1;
	/** The number of active normal allocations */
	protected long activeNormalAllocations = -1;
	/** The number of active huge allocations */
	protected long activeHugeAllocations = -1;

	/** The number of chunk lists for the pooled allocator */
	protected int chunkLists = -1;
	/** The number of small allocation sub-pages for the pooled allocator */
	protected int smallSubPages = -1;
	/** The number of tiney allocation sub-pages for the pooled allocator */
	protected int tinySubPages = -1;

	/** The total chunk size allocation for this allocator */
	protected long totalChunkSize = -1;
	/** The free chunk size allocation for this allocator */
	protected long chunkFreeBytes = -1;
	/** The used chunk size allocation for this allocator */
	protected long chunkUsedBytes = -1;
	/** The percent chunk utilization for this allocator */
	protected int chunkUsage = -1;
	
	/** The elapsed time of the last stats aggregation */
	protected long lastElapsed = 0;
	
	/** The arena metric being monitored */
	protected final PooledByteBufAllocator pooledAllocator;
	/** Indicates if the arena is direct or heap */
	protected final boolean direct;
	/** The name of the type of arena */
	protected final String type;
	/** The JMX ObjectName for this monitor */
	protected final ObjectName objectName;
	/** The schedule handle */
	protected final ScheduledFuture<?> schedulerHandle;
	
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());


	/**
	 * Creates a new BufferArenaMonitor
	 * @param pooledAllocator The allocator being monitored
	 * @param direct true if direct, false if heap
	 */
	BufferArenaMonitor(final PooledByteBufAllocator pooledAllocator, final boolean direct, final boolean registerMBean) {		
		this.pooledAllocator = pooledAllocator;
		this.direct = direct;
		this.type = direct ? "Direct" : "Heap";

		objectName = objectName("net.opentsdb.buffers:service=BufferManagerStats,type=" + type);
		run();
		schedulerHandle = scheduler.scheduleAtFixedRate(this, 5, 5, TimeUnit.SECONDS);
		if(registerMBean) {
			try {
				ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
			} catch (Exception ex) {
				log.warn("Failed to register JMX MBean for [" + type + "] BufferArena. Continuing without", ex);
			}
		}
	}
	
	void stop() {
		if(scheduler!=null) {
			schedulerHandle.cancel(true);
		}
		if(ManagementFactory.getPlatformMBeanServer().isRegistered(objectName)) {
			try { ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectName); } catch (Exception x) {/* No Op */}
		}
	}
	
	private static ObjectName objectName(final String name) {
		try {
			return new ObjectName(name);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	
	
	/**
	 * <p>Refreshes the arena stats</p>
	 * {@inheritDoc}
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		final long start = System.currentTimeMillis();
		final List<PoolArenaMetric> arenaMetrics = direct ? pooledAllocator.directArenas() : pooledAllocator.heapArenas();
		
		totalChunkSize = 0;
		chunkFreeBytes = 0;
		chunkUsedBytes = 0;
		chunkUsage = 0;
		
		
		chunkLists = 0;
		smallSubPages = 0;
		tinySubPages = 0;		
		allocations = 0;
		tinyAllocations = 0;
		smallAllocations = 0;
		normalAllocations = 0;
		hugeAllocations = 0;
		deallocations = 0;
		tinyDeallocations = 0;
		smallDeallocations = 0;
		normalDeallocations = 0;
		hugeDeallocations = 0;
		activeAllocations = 0;
		activeTinyAllocations = 0;
		activeSmallAllocations = 0;
		activeNormalAllocations = 0;
		activeHugeAllocations = 0;		
		for(PoolArenaMetric poolArenaMetric: arenaMetrics) {
			for(PoolChunkListMetric pclm: poolArenaMetric.chunkLists()) {
				for(Iterator<PoolChunkMetric> iter = pclm.iterator(); iter.hasNext();) {
					PoolChunkMetric pcm = iter.next();
					totalChunkSize += pcm.chunkSize();
					chunkFreeBytes += pcm.freeBytes();
				}
			}		
			chunkLists += poolArenaMetric.chunkLists().size();
			smallSubPages += poolArenaMetric.smallSubpages().size();
			tinySubPages += poolArenaMetric.tinySubpages().size();					
			allocations += poolArenaMetric.numAllocations();
			tinyAllocations += poolArenaMetric.numTinyAllocations();
			smallAllocations += poolArenaMetric.numSmallAllocations();
			normalAllocations += poolArenaMetric.numNormalAllocations();
			hugeAllocations += poolArenaMetric.numHugeAllocations();
			deallocations += poolArenaMetric.numDeallocations();
			tinyDeallocations += poolArenaMetric.numTinyDeallocations();
			smallDeallocations += poolArenaMetric.numSmallDeallocations();
			normalDeallocations += poolArenaMetric.numNormalDeallocations();
			hugeDeallocations += poolArenaMetric.numHugeDeallocations();
			activeAllocations += poolArenaMetric.numActiveAllocations();
			activeTinyAllocations += poolArenaMetric.numActiveTinyAllocations();
			activeSmallAllocations += poolArenaMetric.numActiveSmallAllocations();
			activeNormalAllocations += poolArenaMetric.numActiveNormalAllocations();
			activeHugeAllocations += poolArenaMetric.numActiveHugeAllocations();
		}
		chunkUsedBytes = totalChunkSize - chunkFreeBytes;		
		chunkUsage = perc(totalChunkSize, chunkUsedBytes);
		
		lastElapsed = System.currentTimeMillis() - start;
		//log.debug("Collected [{}] allocator stats from [{}] arena metrics in {} ms.", type, arenaMetrics.size(), lastElapsed);
	}
	
	private static int perc(final double total, final double part) {
		if(total==0 || part==0) return 0;
		final double d = part/total * 100D;
		return (int)d;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getAllocations()
	 */
	@Override
	public long getAllocations() {
		return allocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinyAllocations()
	 */
	@Override
	public long getTinyAllocations() {
		return tinyAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallAllocations()
	 */
	@Override
	public long getSmallAllocations() {
		return smallAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getNormalAllocations()
	 */
	@Override
	public long getNormalAllocations() {
		return normalAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getHugeAllocations()
	 */
	@Override
	public long getHugeAllocations() {
		return hugeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getDeallocations()
	 */
	@Override
	public long getDeallocations() {
		return deallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinyDeallocations()
	 */
	@Override
	public long getTinyDeallocations() {
		return tinyDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallDeallocations()
	 */
	@Override
	public long getSmallDeallocations() {
		return smallDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getNormalDeallocations()
	 */
	@Override
	public long getNormalDeallocations() {
		return normalDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getHugeDeallocations()
	 */
	@Override
	public long getHugeDeallocations() {
		return hugeDeallocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveAllocations()
	 */
	@Override
	public long getActiveAllocations() {
		return activeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveTinyAllocations()
	 */
	@Override
	public long getActiveTinyAllocations() {
		return activeTinyAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveSmallAllocations()
	 */
	@Override
	public long getActiveSmallAllocations() {
		return activeSmallAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveNormalAllocations()
	 */
	@Override
	public long getActiveNormalAllocations() {
		return activeNormalAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getActiveHugeAllocations()
	 */
	@Override
	public long getActiveHugeAllocations() {
		return activeHugeAllocations;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getType()
	 */
	@Override
	public String getType() {
		return type;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#isDirect()
	 */
	@Override
	public boolean isDirect() {
		return direct;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkLists()
	 */
	@Override
	public int getChunkLists() {
		return chunkLists;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getSmallSubPages()
	 */
	@Override
	public int getSmallSubPages() {
		return smallSubPages;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTinySubPages()
	 */
	@Override
	public int getTinySubPages() {
		return tinySubPages;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getTotalChunkSize()
	 */
	@Override
	public long getTotalChunkSize() {
		return totalChunkSize;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkFreeBytes()
	 */
	@Override
	public long getChunkFreeBytes() {
		return chunkFreeBytes;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkUsedBytes()
	 */
	@Override
	public long getChunkUsedBytes() {		
		return chunkUsedBytes;
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getChunkUsage()
	 */
	@Override
	public int getChunkUsage() {
		return chunkUsage;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.streams.buffers.BufferArenaMonitorMBean#getLastElapsed()
	 */
	@Override
	public long getLastElapsed() {
		return lastElapsed;
	}

}
