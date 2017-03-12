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
package net.opentsdb.client.tracing;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * <p>Title: Tracer</p>
 * <p>Description: Defines the base data tracer which supports sending data points to OpenTSDB</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.tracing.Tracer</code></p>
 */

public interface Tracer {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
    /**
     * Formats and sends the specified datapoint to OpenTSDB
     * @param time The timestamp of the metric
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
     */
    public void trace(long time, Object metric, long value, Map<Object, Object> tags);

    /**
     * Formats and sends the specified datapoint to OpenTSDB
     * @param time The timestamp of the metric
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
     */
    public void trace(long time, Object metric, double value, Map<Object, Object> tags);
    
    /**
     * Formats and sends the specified datapoint to OpenTSDB using the current timestamp
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
     */
    public void trace(Object metric, long value, Map<Object, Object> tags);

    /**
     * Formats and sends the specified datapoint to OpenTSDB using the current timestamp
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
     */
    public void trace(Object metric, double value, Map<Object, Object> tags);
    
    /**
     * <p>Parses and traces a full <code>put</code> measurement. e.g.</p>
     * <p><b><code>tsd.jvm.thread.pool.block.count 1489082980 0 host=HeliosLeopard name=TCP-EpollBoss</code></b></p>
     * @param fullMetric
     */
    public void trace(String fullMetric);
    
    /**
     * Flushes the accumulated metrics
     */
    public void flush();

}
