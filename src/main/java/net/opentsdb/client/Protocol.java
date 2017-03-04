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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.compression.JZlibEncoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.string.StringDecoder;
import net.opentsdb.client.tracing.TraceCodec;

/**
 * <p>Title: Protocol</p>
 * <p>Description: Functional enumeration of the supported protocols</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.Protocol</code></p>
 */

public enum Protocol implements ClientBuilder {
	TCP(new TCPClientBuilder(), false),
	UDP(new UDPClientBuilder(), false),
	UNIX(new UnixClientBuilder(), true);
	
	
	private Protocol(final ClientBuilder builder, final boolean requiresEpoll) {
		this.builder = builder;
		this.requiresEpoll = requiresEpoll;
	}
	
	final ClientBuilder builder;
	public final boolean requiresEpoll;
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.ClientBuilder#fromString(java.lang.String)
	 */
	@Override
	public SocketAddress fromString(final String address) {
		return builder.fromString(address);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.ClientBuilder#channelClass(net.opentsdb.client.ClientConfiguration)
	 */
	@Override
	public Class<? extends Channel> channelClass(final ClientConfiguration config) {
		return builder.channelClass(config);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.ClientBuilder#channelInitializer(net.opentsdb.client.ClientConfiguration, net.opentsdb.client.CallbackHandler)
	 */
	@Override
	public ChannelInitializer<? extends Channel> channelInitializer(final ClientConfiguration config, final CallbackHandler callbackHandler) {
		return builder.channelInitializer(config, callbackHandler);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.ClientBuilder#eventLoopGroup(net.opentsdb.client.ClientConfiguration)
	 */
	@Override
	public EventLoopGroup eventLoopGroup(ClientConfiguration config) {		
		return builder.eventLoopGroup(config);
	}
	
	public abstract static class BaseClientBuilder implements ClientBuilder {
		static final Charset UTF8 = Charset.forName("UTF8");
		static final StringDecoder stringDecoder = new StringDecoder(UTF8);
		ChannelInitializer<? extends Channel> initializer = null;
		final AtomicBoolean inited = new AtomicBoolean(false);
		
		@Override
		public SocketAddress fromString(final String address) {
			if(address==null || address.trim().isEmpty()) throw new IllegalArgumentException("The passed address was null or empty");
			final int index = address.indexOf('/');
			final String[] frags = index==-1 ? address.trim().split(":") : address.substring(index+1).trim().split(":");
//			final String[] frags = address.trim().split(":");
			return new InetSocketAddress(frags[0].trim(), Integer.parseInt(frags[1].trim()));
		}
		
		@Override
		public ChannelInitializer<? extends Channel> channelInitializer(final ClientConfiguration config, final CallbackHandler callbackHandler) {
			if(inited.compareAndSet(false, true)) {
				initializer = new ChannelInitializer<Channel>() {
					@Override
					protected void initChannel(final Channel ch) throws Exception {
						final ChannelPipeline p = ch.pipeline();
						if(config.encoding()==TraceCodec.JSON) {
							if(config.gzip) {
								p.addLast("Compressor", new HttpContentCompressor());
							}
							p.addLast("HttpCodec", new HttpClientCodec());
							p.addLast("Decompressor", new HttpContentDecompressor());
							p.addLast("Aggregator", new HttpObjectAggregator(1048576));							
						} else {
							if(config.gzip) {
								p.addLast("compressor", new JZlibEncoder(ZlibWrapper.GZIP, 9));
							} else {
								p.addLast("passthrough", new ChannelDuplexHandler());
							}
							p.addLast("stringdecoder", stringDecoder);
						}
						p.addLast("handler", callbackHandler.traceCodec().outboundHandler(callbackHandler));
					}
				};
			}
			return initializer;
		}
		
		@Override
		public EventLoopGroup eventLoopGroup(final ClientConfiguration config) {
			if(config.epoll()) {
				return new EpollEventLoopGroup(1);
			} else {
				return new NioEventLoopGroup(1);
			}			
		}
	}
	
	public static class TCPClientBuilder extends BaseClientBuilder {
		@Override
		public Class<? extends Channel> channelClass(final ClientConfiguration config) {
			if(config.epoll()) {
				return EpollSocketChannel.class;
			} else {
				return NioSocketChannel.class;
			}
		}
	}

	public static class UDPClientBuilder extends BaseClientBuilder {
		@Override
		public Class<? extends Channel> channelClass(final ClientConfiguration config) {
			if(config.epoll()) {
				return EpollDatagramChannel.class;
			} else {
				return NioDatagramChannel.class;
			}
		}		
	}
	
	
	
	public static class UnixClientBuilder extends BaseClientBuilder  {
		@Override
		public SocketAddress fromString(final String address) {
			if(address==null || address.trim().isEmpty()) throw new IllegalArgumentException("The passed address was null or empty");			
			return new DomainSocketAddress(address);
		}
		@Override
		public Class<? extends Channel> channelClass(final ClientConfiguration config) {
			if(!config.epoll()) {
				throw new IllegalArgumentException("UNIX Client not supported. EPoll Disabled");
			}
			return EpollDomainSocketChannel.class;
		}	
	}
	
}





