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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import net.opentsdb.client.buffer.BufferManager;
import net.opentsdb.client.util.SpinLock;


/**
 * <p>Title: RedisClient</p>
 * <p>Description: Redis client for recording metrics</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.redis.RedisClient</code></p>
 */

public class RedisClient extends ChannelInboundHandlerAdapter {
	/** Buffer manager */
	protected static final BufferManager bman = BufferManager.getInstance();
	
	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The HMSET command */
	protected static final RedisMessage HMSET = rm(bman.upwrap("HMSET").asReadOnly());
	/** The HGETALL command */
	protected static final RedisMessage HGETALL = rm(bman.upwrap("HGETALL").asReadOnly());
	/** The HKEYS command */
	protected static final RedisMessage HKEYS = rm(bman.upwrap("HKEYS").asReadOnly());	
	
	/** The SELECT command */
	protected static final RedisMessage SELECT = rm(bman.upwrap("SELECT").asReadOnly());
	/** The KEYS command */
	protected static final RedisMessage KEYS = rm(bman.upwrap("KEYS").asReadOnly());
	/** The KEYS wildcard */
	protected static final RedisMessage KEYSWC = rm(bman.upwrap("*").asReadOnly());
    
    protected final EventLoopGroup group = new NioEventLoopGroup();
    protected final Bootstrap bootstrap = new Bootstrap();
    protected final Channel ch;
    
    protected final ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
    	@Override
    	protected void initChannel(final SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast("decoder", new RedisDecoder());
            p.addLast("string-aggregator", new RedisBulkStringAggregator());
            p.addLast("array-aggregator", new RedisArrayAggregator());
            p.addLast("encoder", new RedisEncoder());
            p.addLast("handler", RedisClient.this);    		
    	}
    };
    
    public RedisClient(final String host, final int port) {
    	bootstrap.group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.ALLOCATOR, bman)
        .handler(initializer);
    	ch = bootstrap.connect(host, port).awaitUninterruptibly().channel();  // FIXME
    	ch.closeFuture().addListener(new ChannelFutureListener(){
    		@Override
    		public void operationComplete(final ChannelFuture f) throws Exception {
    			System.err.println("REDIS CHANNEL CLOSED");
    			if(f.cause()!=null) f.cause().printStackTrace(System.err);
    		}
    	});
    }
    
    protected final AtomicLong reqId = new AtomicLong();
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** Response Queue */
	protected final BlockingQueue<Object> responseQueue = new ArrayBlockingQueue<Object>(128, true);
	/** State spin lock */
	protected final SpinLock lock = new SpinLock();
	/** Ignored response poller */
	protected final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory(){
		protected final AtomicInteger ai = new AtomicInteger(0);
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "IgnoredResponsePoller#" + ai.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});
	
	/** The write result listener factory */
	protected GenericFutureListener<ChannelFuture> writeListener(final String commandType) {
		return new GenericFutureListener<ChannelFuture>() {
	        @Override
	        public void operationComplete(ChannelFuture future) throws Exception {
	            if (!future.isSuccess()) {
	            	log.info(commandType + " failed", future.cause());
	            }
	        }		
		};
	}
	
	
	
	protected <T> Future<T> consumeTask(final String commandType, final Class<T> type) {
		return executor.submit(new Callable<T>(){
			@Override
			public T call() throws Exception {
				try {
					final Object obj = responseQueue.poll(5, TimeUnit.SECONDS);
					log.info("{} Response#{}:[{}]", commandType, reqId.incrementAndGet(), obj);
					if(obj instanceof Throwable) {
						final Throwable t = (Throwable)obj;
						log.error(commandType + " call failed", t);
						throw new Exception(t);
					}
					return (T)obj;
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		});
	}
	
	public void close() {
		ch.close();
		group.shutdownGracefully();
		log("Closed");
	}
	
	public static void main(String[] args) {
		final RedisClient client = new RedisClient("localhost", 6379);
		try {
			log("Connected");
			client.select(13);
			log("Switched to DB 13");
			final Map<String, String> map = client.hmset(Collections.singletonMap("foo", 45), "xxx")
				.hgetall("xxx");
			client.log.info("Results:{}", map);
			
			
			//hgetall
		} finally {
			client.close();
		}
	}
	
	public static void log(final Object fmt, final Object...args) {
		System.out.println(String.format(fmt.toString(), args));
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		final RedisMessage rm = (RedisMessage)msg;
		ReferenceCountUtil.retain(rm);
		responseQueue.add(rm);
//        RedisMessage redisMessage = (RedisMessage) msg;
        //printAggregatedRedisResponse(redisMessage);  TODO
//        ReferenceCountUtil.release(redisMessage);
	}
	
	/**
	 * Invokes an HMSET against the connected Redis instance
	 * @param hash The map of values to write
	 * @param key The keys to be concatenated
	 * @return this client
	 */
	public RedisClient hmset(final Map<String, Object> hash, final String...key) {
		if(!ch.isOpen()) throw new IllegalStateException("The connection is closed");
		List<RedisMessage> msgs = new ArrayList<RedisMessage>(hash.size() + 2);
		msgs.add(HMSET);
		msgs.add(rm(String.join("/", key)));
		for(Map.Entry<String, Object> entry: hash.entrySet()) {
			msgs.add(rm(entry.getKey()));
			msgs.add(rmo(entry.getValue()));
		}		
		ch.writeAndFlush(new ArrayRedisMessage(msgs).retain()).addListener(writeListener("hmset"));
		consumeTask("hmset", null);
		return this;
	}
	
	/**
	 * Switches the current database this client is connected to
	 * @param database The database id to switch to
	 * @return this client
	 */
	public RedisClient select(final int database) {
		if(!ch.isOpen()) throw new IllegalStateException("The connection is closed");
		if(database < 0 || database > 15) throw new IllegalArgumentException("Invalid database index:" + database);
		ch.writeAndFlush(new ArrayRedisMessage(
			new ArrayList<RedisMessage>(Arrays.asList(SELECT, rmo(database))))
		)
		.addListener(writeListener("select"));
		
		try {
			consumeTask("select", null).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this;
	}
	
	public Map<String, String> hgetall(final String...key) {
		if(!ch.isOpen()) throw new IllegalStateException("The connection is closed");
		final List<RedisMessage> msgs = new ArrayList<RedisMessage>(Arrays.asList(
			HGETALL, rm(String.join("/", key))
		));
		ch.writeAndFlush(new ArrayRedisMessage(msgs).retain()).addListener(writeListener("hgetall"));
		try {
			final ArrayRedisMessage arm = consumeTask("hgetall", ArrayRedisMessage.class).get();
			String k = null;
			final Map<String, String> map = new HashMap<String, String>(arm.children().size());
			for(RedisMessage rm : arm.children()) {
				FullBulkStringRedisMessage frm = (FullBulkStringRedisMessage)rm;
				if(k==null) {
					k = frm.content().toString(UTF8);
				} else {
					map.put(k, frm.content().toString(UTF8));
					k = null;
				}
			}
			return map;
		} catch (Exception ex) {
			throw new RuntimeException("hgetall call failed", ex);
		}		
	}
	
	public Set<String> hkeys(final String...key) {
		if(!ch.isOpen()) throw new IllegalStateException("The connection is closed");
		final List<RedisMessage> msgs = new ArrayList<RedisMessage>(Arrays.asList(
				HKEYS, rm(String.join("/", key))
		));
		ch.writeAndFlush(new ArrayRedisMessage(msgs)).addListener(writeListener("hkeys"));
		try {
			final ArrayRedisMessage arm = consumeTask("hkeys", ArrayRedisMessage.class).get();
			final Set<String> set = new LinkedHashSet<String>(arm.children().size());
			for(RedisMessage rm : arm.children()) {
				FullBulkStringRedisMessage frm = (FullBulkStringRedisMessage)rm;
				set.add(frm.content().toString(UTF8));
			}
			return set;
		} catch (Exception ex) {
			throw new RuntimeException("hkeys call failed", ex);
		}		
	}
	
	
	public Set<String> keys(final String pattern) {
		if(!ch.isOpen()) throw new IllegalStateException("The connection is closed");
		final List<RedisMessage> msgs = new ArrayList<RedisMessage>(Arrays.asList(KEYS));
		if(pattern!=null && !pattern.trim().isEmpty()) {
			msgs.add(rm(pattern.trim()));
		} else {
			msgs.add(KEYSWC);
		}
		ch.writeAndFlush(new ArrayRedisMessage(msgs)).addListener(writeListener("keys"));
		try {
			final ArrayRedisMessage arm = consumeTask("keys", ArrayRedisMessage.class).get();
			final LinkedHashSet<String> set = new LinkedHashSet<String>(arm.children().size());
			for(RedisMessage rm : arm.children()) {
				FullBulkStringRedisMessage frm = (FullBulkStringRedisMessage)rm;
				set.add(frm.content().toString(UTF8));
				ReferenceCountUtil.safeRelease(frm);
			}
			return set;
		} catch (Exception ex) {
			throw new RuntimeException("keys call failed", ex);
		}		
	}
	
	
	protected static RedisMessage rm(final CharSequence cs) {
		return new FullBulkStringRedisMessage(bman.wrap(cs)).retain();
	}
	
	protected static RedisMessage rmo(final Object obj) {
		return new FullBulkStringRedisMessage(bman.wrap(obj.toString()));
	}
	
	
	protected static RedisMessage rm(final ByteBuf buf) {
		return new FullBulkStringRedisMessage(buf);
	}
	
}