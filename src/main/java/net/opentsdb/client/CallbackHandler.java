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
package net.opentsdb.client;



import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.http.HttpObject;
import net.opentsdb.client.tracing.TraceCodec;

/**
 * <p>Title: CallbackHandler</p>
 * <p>Description: Tracing response callback handler</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.CallbackHandler</code></p>
 */

public interface CallbackHandler {
	/**
	 * Callback handler for Http responses
	 * @param response the response
	 */
	public void onResponse(HttpObject response);

	/**
	 * Callback handler for String responses
	 * @param response the response
	 */
	public void onResponse(String...response);
	
	/**
	 * Callback handler for UDP responses
	 * @param response the response
	 */
	public void onResponse(DatagramPacket response);
	
	/**
	 * Callback handler for ByteBuf responses
	 * @param response the response
	 */
	public void onResponse(ByteBuf response);
	
	/**
	 * Returns the callback handler's codec
	 * @return the callback handler's codec
	 */
	public TraceCodec traceCodec();
	
}
