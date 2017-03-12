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
package net.opentsdb.client.protocol;

import java.net.SocketAddress;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import net.opentsdb.client.CallbackHandler;
import net.opentsdb.client.ClientConfiguration;

/**
 * <p>Title: ClientBuilder</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.ClientBuilder</code></p>
 */

public interface ClientBuilder {

	public SocketAddress fromString(String address);
	
	public EventLoopGroup eventLoopGroup(final ClientConfiguration config);
	
	public Class<? extends Channel> channelClass(final ClientConfiguration config);
	
	public ChannelInitializer<? extends Channel> channelInitializer(final ClientConfiguration config, final CallbackHandler callbackHandler);
	
	public BaseClient newClient(final ClientConfiguration config);
	
	
}
