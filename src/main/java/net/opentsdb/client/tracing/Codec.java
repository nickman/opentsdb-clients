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

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

import io.netty.channel.SimpleChannelInboundHandler;
import net.opentsdb.client.CallbackHandler;

/**
 * <p>Title: Encoder</p>
 * <p>Description: Defines the encoder that accepts traces and encodes for transmission</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.tracing.Codec</code></p>
 */

public interface Codec {
	/** The UTF8 character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/**
	 * Writes any required prefix when the stream is first opened
	 * @param out the The OutputStream to write the header to
	 */
	public void header(OutputStream out);
	
	/**
	 * Writes any required closer before the stream is closed
	 * @param out the The OutputStream to write the closer to
	 */
	public void tailer(OutputStream out);
	
	/**
	 * Encodes the passed data point
	 * @param buffer The OutputStream to write the encoded data point into
     * @param time The timestamp of the metric
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
	 */
	public void encode(OutputStream out, long time, String metric, long value, Map<String, String> tags);
	
	/**
	 * Encodes the passed data point
	 * @param buffer The OutputStream to write the encoded data point into
     * @param time The timestamp of the metric
     * @param metric The metric name
     * @param value The value
     * @param tags The metric tags
	 */
	public void encode(OutputStream out, long time, String metric, double value, Map<String, String> tags);
	
	/**
	 * Returns an inbound handler suitable for this codec
	 * @return an inbound handler
	 */
	public <T> SimpleChannelInboundHandler<T> outboundHandler(CallbackHandler callbackHandler);
}
