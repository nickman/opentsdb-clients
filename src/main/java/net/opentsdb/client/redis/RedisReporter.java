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
package net.opentsdb.client.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * <p>Title: RedisReporter</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.redis.RedisReporter</code></p>
 */

public class RedisReporter implements Reporter {
	protected final String host;
	protected final int port;
	
	protected final RedisClient rc;
	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());

	
	public RedisReporter(final String host, final int port, final int db) {		
		this.host = host;
		this.port = port;
		rc = new RedisClient(host, port);
		rc.select(db);
	}
	
	public RedisReporter(final String host, final int port) {
		this(host, port, 11);
	}
	
	public RedisReporter() {
		this("localhost", 6379);
	}
	
	
	
	public void report(final String testId, final MetricRegistry registry) {
		final long startTime = System.currentTimeMillis();
		final Map<String, Object> stats = new HashMap<String, Object>(512);
		for(Map.Entry<String, Timer> timer: registry.getTimers().entrySet()) {
			final Timer t = timer.getValue();
			final String prefix = "timer." + timer.getKey() + ".";
			stats.put(prefix + "count", t.getCount());
			stats.put(prefix + "15mRate", t.getFifteenMinuteRate());
			stats.put(prefix + "5mRate", t.getFiveMinuteRate());
			stats.put(prefix + "1mRate", t.getOneMinuteRate());
			stats.put(prefix + "meanRate", t.getMeanRate());
			final Snapshot sn = t.getSnapshot();
			stats.put(prefix + "999pct", sn.get999thPercentile());
			stats.put(prefix + "99pct", sn.get99thPercentile());
			stats.put(prefix + "98pct", sn.get98thPercentile());
			stats.put(prefix + "95pct", sn.get95thPercentile());
			stats.put(prefix + "75pct", sn.get75thPercentile());
//			stats.put(prefix + "max", sn.getMax());
//			stats.put(prefix + "min", sn.getMin());
			stats.put(prefix + "median", sn.getMedian());
			stats.put(prefix + "mean", sn.getMean());
//			stats.put(prefix + "stddev", sn.getStdDev());			
		}
		for(Map.Entry<String, Meter> meter: registry.getMeters().entrySet()) {
			final Meter m = meter.getValue();
			final String prefix = "meter." + meter.getKey() + ".";
			stats.put(prefix + "count", m.getCount());
			stats.put(prefix + "15mRate", m.getFifteenMinuteRate());
			stats.put(prefix + "5mRate", m.getFiveMinuteRate());
			stats.put(prefix + "1mRate", m.getOneMinuteRate());
			stats.put(prefix + "meanRate", m.getMeanRate());			
		}
		for(Map.Entry<String, Histogram> hist: registry.getHistograms().entrySet()) {
			final Histogram h = hist.getValue();
			final String prefix = "histogram." + hist.getKey() + ".";
			stats.put(prefix + "count", h.getCount());
			final Snapshot sn = h.getSnapshot();
			stats.put(prefix + "999pct", sn.get999thPercentile());
			stats.put(prefix + "99pct", sn.get99thPercentile());
			stats.put(prefix + "98pct", sn.get98thPercentile());
			stats.put(prefix + "95pct", sn.get95thPercentile());
			stats.put(prefix + "75pct", sn.get75thPercentile());
//			stats.put(prefix + "max", sn.getMax());
//			stats.put(prefix + "min", sn.getMin());
			stats.put(prefix + "median", sn.getMedian());
			stats.put(prefix + "mean", sn.getMean());
//			stats.put(prefix + "stddev", sn.getStdDev());			
		}
		for(Map.Entry<String, Counter> counter: registry.getCounters().entrySet()) {
			final Counter c = counter.getValue();
			final String prefix = "counter." + counter.getKey() + ".";
			stats.put(prefix + "count", c.getCount());			
		}		
		rc.hmset(stats, testId);
		log.info("Saved stats to redis in {} ms", System.currentTimeMillis() - startTime);		
	}
	
	public static void main(String[] args) {
		final RedisReporter rr = new RedisReporter();
		try {
			rr.writeToFile();
		} finally {
			try { rr.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	public void writeToFile() {
//		rc.select(11);
		final Set<String> keys = rc.keys(null);
		final Set<String> sortedKeys = new TreeSet<String>(keys);
		StringBuilder b = new StringBuilder("KEYS:");
		for(String key: sortedKeys) {
			b.append("\n\t").append(key);
		}		
		b.append("\n");
		//log.info(b.toString());
		final Set<String> hashKeys = new TreeSet<String>(rc.hkeys(sortedKeys.iterator().next()));
		//log.info(b.toString());
		final StringBuilder rep = new StringBuilder(4096);
		rep.append("\n");
		rep.append("Protocol,ChannelType,Encoding,ByteBufType,ByteByfAlloc,");
		for(String key: hashKeys) {
			rep.append(key).append(",");
		}		
		rep.deleteCharAt(b.length()-1).append("\n");
		for(String testName: sortedKeys) {
			final String[] frags = testName.split("/");
			final Map<String, String> values = rc.hgetall(testName);
			for(String frag: frags) {
				rep.append(frag).append(",");
			}			
			for(String key: hashKeys) {
				rep.append(values.get(key)).append(",");
			}
			rep.deleteCharAt(b.length()-1).append("\n");
		}
		log.info(rep.toString());
	}



	public void close() {
		rc.close();
		
	}
	
	
}
