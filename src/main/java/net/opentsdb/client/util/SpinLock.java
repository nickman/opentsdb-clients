// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
package net.opentsdb.client.util;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>Title: SpinLock</p>
 * <p>Description: A simple non-blocking spin lock</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.util.SpinLock</code></p>
 */

public class SpinLock {
	/** The unlocked value for the spin lock */
	private static final long UNLOCKED = -1L;
	/** The lock holder */
	private final AtomicLong lock = new AtomicLong(UNLOCKED);
	
	/**
	 * Creates a new SpinLock
	 */
	public SpinLock() {
		
	}
	
	/**
	 * Acquires this lock for the calling thread.
	 * @param barge true for a high priority acquire, false for normal
	 * @return true if lock was already locked by the calling thread, false otherwise
	 */
	public boolean lock(final boolean barge) {
		final long id = Thread.currentThread().getId();
		if(lock.get()!=id) {
			while(lock.compareAndSet(UNLOCKED, id)) {
				if(!barge) Thread.yield();
			}
			return false;
		}
		return true;
	}
	
	/**
	 * Acquires this lock for the calling thread using normal priority.
	 * @return true if lock was already locked by the calling thread, false otherwise
	 */
	public boolean lock() {
		return lock(false);
	}
	
	
	/**
	 * Releases the lock if held by the calling thread
	 */
	public void unlock() {
		lock.compareAndSet(Thread.currentThread().getId(), UNLOCKED);
	}
	
	/**
	 * Executes the passed callable after acquiring the lock.
	 * @param barge true for a high priority acquire, false for normal
	 * @param task The task to execute
	 * @return the return value of the task
	 */
	public <T> T doInLock(final boolean barge, final Callable<T> task) {
		if(task==null) throw new IllegalArgumentException("The passed task was null");
		boolean alreadyLocked = true;
		try {
			alreadyLocked = lock(barge);
			try {
				return task.call();
			} catch (Exception ex) {
				throw new RuntimeException("doInLock task failed", ex);
			}
		} finally {
			if(!alreadyLocked) unlock();
		}
	}
	
	/**
	 * Executes the passed callable after acquiring the lock using normal priority.
	 * @param task The task to execute
	 * @return the return value of the task
	 */
	public <T> T doInLock(final Callable<T> task) {
		return doInLock(false, task);
	}
	
	/**
	 * Executes the passed runnable after acquiring the lock.
	 * @param barge true for a high priority acquire, false for normal
	 * @param task The task to execute
	 */
	public void doInLock(final boolean barge, final Runnable task) {
		if(task==null) throw new IllegalArgumentException("The passed task was null");
		boolean alreadyLocked = true;
		try {
			alreadyLocked = lock(barge);
			try {
				task.run();
			} catch (Exception ex) {
				throw new RuntimeException("doInLock runnable task failed", ex);
			}
		} finally {
			if(!alreadyLocked) unlock();
		}
	}
	
	/**
	 * Executes the passed runnable after acquiring the lock using normal priority.
	 * @param task The task to execute
	 */
	public void doInLock(final Runnable task) {
		doInLock(false, task);
	}

}
