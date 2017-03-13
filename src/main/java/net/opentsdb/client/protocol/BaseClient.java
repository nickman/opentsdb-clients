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

import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.streams.common.naming.AgentName;
import com.heliosapm.streams.common.naming.AgentNameChangeListener;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import net.opentsdb.client.CallbackHandler;
import net.opentsdb.client.ClientConfiguration;
import net.opentsdb.client.Protocol;
import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.tracing.TraceCodec;
import net.opentsdb.client.tracing.Tracer;
import net.opentsdb.client.util.SpinLock;
import net.opentsdb.client.util.StringsUtil;

/**
 * <p>Title: BaseClient</p>
 * <p>Description: The abstract base client</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.protocol.BaseClient</code></p>
 */

public abstract class BaseClient<T extends BaseClient<T>> implements Tracer, CallbackHandler, MetricSet, AgentNameChangeListener {
	/** Indicates if we're using ms timestamps (true) or second timestamps (false) */
	protected final boolean msTime;
	/** Time unit for timestamp conversion */
	protected final TimeUnit conversion;
	/** Indicates if we're using direct allocation byte buffers (true) or heap buffers (false) */
	protected final boolean directBuffers;
	/** Indicates if we're using pooled buffers (true) or unpooled buffers (false) */
	protected final boolean pooledBuffers;
	/** The data point codec */
	protected final TraceCodec codec;
	/** The byte buf allocator */
	protected final ByteBufAllocator allocator;
	/** The initial size of the trace buffer */
	protected final int initialTraceBufferSize;
	/** Indicates if gzip is enabled */
	protected final boolean gzipEnabled;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The buffer tracing events are written into */
	protected ByteBuf traceBuffer = null;
	/** The tracer buffer's OutputStream */
	protected ByteBufOutputStream output = null;
	
	/** The spin lock guarding the tracer buffer */
	protected final SpinLock spinLock = new SpinLock();
	/** The client protocol */
	protected final Protocol protocol;
	/** The socket address to connect to */
	protected final SocketAddress socketAddress;
	
	
	protected final LongAdder totalDatapointsSent = new LongAdder();
	protected final LongAdder currentBatchSize = new LongAdder();
	
	protected final MetricRegistry registry = new MetricRegistry();
	protected final Timer swapTimer = registry.timer("swap");
	protected final Timer traceFlushTimer = registry.timer("flush");
	protected final Meter datapointMeter = registry.meter("datapoints");
	protected final Counter traceErrorCounter = registry.counter("traceErrors");
	protected final Counter sendCounter = registry.counter("sendCount");
	protected final Counter invalidTraceCounter = registry.counter("invalidTraceCount");
	protected final Timer importTimer = registry.timer("import");
	protected final Histogram importAlloc = registry.histogram("importAlloc");
	protected final Histogram bytesSent = registry.histogram("bytesSent");
	
	protected final LongAdder bytesEncoded = new LongAdder();
	
	protected final JmxReporter reporter = JmxReporter.forRegistry(registry)
			.registerWith(ManagementFactory.getPlatformMBeanServer())
			.build();
	
	protected final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(registry).build();
	
	
	protected Bootstrap bootstrap = new Bootstrap();
	protected EventLoopGroup group = null;
	protected ChannelInitializer<? extends Channel> initializer = null;
	protected Class<? extends Channel> channelClass = null;
	protected Channel channel = null;
	
	protected final ClientConfiguration clientConfig;
	protected final boolean async;
	
	protected final Map<String, Metric> registeredMetrics;
	protected final AtomicBoolean metricsEnabled = new AtomicBoolean(false);
	protected final AtomicBoolean initialized = new AtomicBoolean(false);
	
	protected final AtomicReference<String> hostName = new AtomicReference<String>(null); 
	protected final AtomicReference<String> appName = new AtomicReference<String>(null);
	
	protected final AtomicReference<CountDownLatch> responseLatch = new AtomicReference<CountDownLatch>(null);
	
	protected final AgentName agentName = AgentName.getInstance(); 

	static {		
		InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
	}
	
	/**
	 * Creates a new BaseClient
	 * @param msTime Indicates if we're using ms timestamps (true) or second timestamps (false)
	 * @param directBuffers Indicates if we're using direct allocation byte buffers (true) or heap buffers (false)
	 * @param pooledBuffers Indicates if we're using pooled buffers (true) or unpooled buffers (false)
	 * @param codec The data point encoding
	 */
	protected BaseClient(final ClientConfiguration clientConfiguration) {
		if(clientConfiguration==null) throw new IllegalArgumentException("The passed ClientConfiguration was null");
		hostName.set(agentName.getHostName());
		appName.set(agentName.getAppName());
		agentName.addAgentNameChangeListener(this);
		clientConfig = clientConfiguration;
		async = clientConfiguration.async();
		protocol = clientConfiguration.protocol();		
		msTime = clientConfiguration.msTime();		
		directBuffers = clientConfiguration.directBuffers();
		pooledBuffers = clientConfiguration.pooledBuffers();
		codec = clientConfiguration.encoding();
		socketAddress = clientConfiguration.address();
		gzipEnabled = clientConfiguration.gzip();
		initialTraceBufferSize = clientConfiguration.traceBufferSize();
		allocator = pooledBuffers ? new PooledByteBufAllocator(directBuffers) : new UnpooledByteBufAllocator(directBuffers);
		conversion = msTime ? TimeUnit.MILLISECONDS : TimeUnit.SECONDS;
		group = protocol.eventLoopGroup(clientConfiguration);
		initializer = protocol.channelInitializer(clientConfiguration, this);
		channelClass = protocol.channelClass(clientConfiguration);
		bootstrap
			.channel(channelClass)
			.group(group)
			.handler(initializer);
		registeredMetrics = new HashMap<String, Metric>(registry.getMetrics());
		final ChannelFuture connectFuture;
		if(protocol==Protocol.UDP) {
			connectFuture =  bootstrap.bind(0);
		} else {
			connectFuture = bootstrap.connect(socketAddress);
		}
		if(!connectFuture.awaitUninterruptibly(5, TimeUnit.SECONDS)) {
			throw new RuntimeException("Timed out waiting for connect to [" + socketAddress + "]");
		}
		channel =  connectFuture.channel();
		log.info("\n\t==========================\n\t[{}]:[{}] Connected to [{}]\n\t==========================\n", hostName.get(), appName.get(), socketAddress);
		
		reporter.start();
	}
	
	@SuppressWarnings("unchecked")
	public T initialize() {
		if(initialized.compareAndSet(false, true)) {
			swap();
		}
		return (T)this;
	}
	
	public void close() {
		try { channel.close().sync(); } catch (Exception x) {/* No Op */}
		try { group.shutdownGracefully(); } catch (Exception x) {/* No Op */}
	}
	
	@Override
	public void onAgentNameChange(final String app, final String host) {
		hostName.set(host);
		appName.set(app);
		log.info("\n\t=========================\n\tHost/App Changed: [{}]:[{}]\n\t=========================\n", host, app);
		
	}
	
	@Override
	public Map<String, Metric> getMetrics() {		
		return Collections.unmodifiableMap(registeredMetrics);
	}
	
	public void printStats() {
		consoleReporter.report(registry.getGauges(), registry.getCounters(), registry.getHistograms(), registry.getMeters(), registry.getTimers());
	}
	
	public boolean areMetricsEnabled() {
		return metricsEnabled.get();
	}
	
	public void enableMetrics(final boolean enabled) {
		metricsEnabled.set(enabled);
	}
	
	public void requestStats() {
		
	}
	
	protected void onStats(final ByteBuf buf) {
		
	}
	
	/**
	 * Swaps out the current trace buffer and output stream with a new one.
	 * @return the prior trace buffer or null if one did not exist.
	 * @throws Exception
	 */
	protected ByteBuf swap() {
		final Context ctx = swapTimer.time();
		try {
			return spinLock.doInLock(true, new Callable<ByteBuf>(){
				@Override
				public ByteBuf call() throws Exception {
					final ByteBuf buff = allocator.buffer(initialTraceBufferSize);
					final ByteBufOutputStream buffOut = new ByteBufOutputStream(buff);
					ByteBuf retBuf = null;
					if(output!=null) {
						try { 
							writeTailer();
							output.flush();
							retBuf = traceBuffer;
						} catch (Exception ex) {
							currentBatchSize.reset();
							log.error("Failed to flush tracer output stream. Discarding.", ex);
						} finally {
							try { output.close(); } catch (Exception x) {/* No Op */}
							JSONOps.invalidate(output);
							output = null;						
						}
					}
					traceBuffer = buff;
					output = buffOut;
					writeHeader();
					return retBuf;
				}
			});
		} finally {
			if(metricsEnabled.get()) {
				ctx.stop();
			}
		}
	}
	
	protected void writeHeader() {
		codec.header(output);
	}

	protected void writeTailer() {
		codec.tailer(output);
	}
	
	  /**
	   * Ensures that a given string is a valid metric name or tag name/value.
	   * @param what A human readable description of what's being validated.
	   * @param s The string to validate.
	   * @throws IllegalArgumentException if the string isn't valid.
	   */
	  public static String validateString(final String what, final String s) {
	    if (s == null) {
	      throw new IllegalArgumentException("Invalid " + what + ": null");
	    } else if ("".equals(s)) {
	      throw new IllegalArgumentException("Invalid " + what + ": empty string");
	    }
	    final int n = s.length();
	    for (int i = 0; i < n; i++) {
	      final char c = s.charAt(i);
	      if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') 
	          || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' 
	          || c == '/' || Character.isLetter(c))) {
	        throw new IllegalArgumentException("Invalid " + what
	            + " (\"" + s + "\"): illegal character: " + c);
	      }
	    }
	    return s;
	  }	
	
	
	protected Map<String, String> cleanTags(final Map<Object, Object> tags) {
		final Map<String, String> map = new HashMap<String, String>(tags.size());
		for(Map.Entry<Object, Object> entry: tags.entrySet()) {
			map.put(
				validateString("tag key", entry.getKey().toString().trim()),
				validateString("tag value", entry.getValue().toString().trim())
			);
		}
		if(!map.containsKey("host")) {
			map.put("host", hostName.get());
		}
		if(!map.containsKey("app")) {
			map.put("app", appName.get());
		}		
		return map;
	}
	
	protected String cleanMetric(final Object metric) {
		return validateString("metric", metric.toString().trim());
	}


	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(java.lang.String)
	 */
	@Override
	public void trace(final String fullMetric) {
		if(fullMetric==null) throw new IllegalArgumentException("The passed fullMetric was null");
		final String[] frags = StringsUtil.splitString(fullMetric, ' ');
		final int len = frags.length;
		if(len < 4) throw new IllegalArgumentException("Invalid full metric: " + Arrays.toString(frags));
		final Map<Object, Object> tags = new HashMap<Object, Object>(len - 3);
		String[] nv = null;
		for(int i = 3; i < len; i++) {
			nv = StringsUtil.splitString(frags[i], '=');
			tags.put(nv[0], nv[1]);
		}
		if(frags[2].indexOf('.')!=-1) {
			trace(Long.parseLong(frags[1]), frags[0], Double.parseDouble(frags[2]), tags);
		} else {
			trace(Long.parseLong(frags[1]), frags[0], Long.parseLong(frags[2]), tags);
		}
		//    tsd.jvm.thread.pool.block.count 1489082980 0 host=HeliosLeopard name=TCP-EpollBoss
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(long, java.lang.Object, long, java.util.Map)
	 */
	@Override
	public void trace(final long time, final Object metric, final long value, final Map<Object, Object> tags) {
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				try {
					codec.encode(output, time, cleanMetric(metric), value, cleanTags(tags));
					totalDatapointsSent.increment();
				} catch (IllegalArgumentException iae) {
					invalidTraceCounter.inc();
					log.debug("Invalid trace: {}:{}", metric, tags);
				}
			}
		});
		currentBatchSize.increment();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(long, java.lang.Object, double, java.util.Map)
	 */
	@Override
	public void trace(final long time, final Object metric, final double value, final Map<Object, Object> tags) {
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				try {
					codec.encode(output, time, cleanMetric(metric), value, cleanTags(tags));
					totalDatapointsSent.increment();
				} catch (IllegalArgumentException iae) {
					invalidTraceCounter.inc();
					log.debug("Invalid trace: {}:{}", metric, tags);
				}
			}
		});
		currentBatchSize.increment();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(java.lang.Object, long, java.util.Map)
	 */
	@Override
	public void trace(final Object metric, final long value, final Map<Object, Object> tags) {
		trace(time(), metric, value, tags);		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(java.lang.Object, double, java.util.Map)
	 */
	@Override
	public void trace(final Object metric, final double value, final Map<Object, Object> tags) {
		trace(time(), metric, value, tags);		
	}
	
	/**
	 * Returns the current time in ms. or s. depending on config
	 * @return the current time
	 */
	protected long time() {
		return conversion.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	public static final Charset UTF8 = Charset.forName("UTF8");
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#flush()
	 */
	@Override
	public synchronized void flush() {
		waitOnLatch(2000);
		final long dpoints = currentBatchSize.sumThenReset();
		final Context ctx = traceFlushTimer.time();
		final ByteBuf flushedBuffer = swap();
		if(flushedBuffer!=null && flushedBuffer.readableBytes() > 0) {
			final CountDownLatch latch = new CountDownLatch(1);
			if(!async) responseLatch.set(latch);
			send(flushedBuffer).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(final ChannelFuture f) throws Exception {					
					if(f.isSuccess()) {
						if(metricsEnabled.get()) {
							ctx.close();
							datapointMeter.mark(dpoints);
						}
						//totalDatapointsSent.add(dpoints);
					}
				}
			});
			if(!async) {
				try {
					if(!latch.await(5000, TimeUnit.SECONDS)) {
						throw new RuntimeException("Timeout while waiting on response");
					}
				} catch (InterruptedException ex) {
					throw new RuntimeException("Thread interrupted while waiting on response");
				}
			}
		}
	}
	
	public String printCurrentBuffer() {
		return spinLock.doInLock(new Callable<String>(){
			@Override
			public String call() throws Exception {				
				return traceBuffer.toString(UTF8);
			}
		});
	}
	
	
	public long getDatapointsSent() {
		return datapointMeter.getCount();
	}
	
	/**
	 * Sends the accumulated payload
	 * @param sendBuffer The accumulated buffer
	 * @return The write's ChannelFuture
	 */
	protected ChannelFuture send(final ByteBuf sendBuffer) {
		final long bytes = sendBuffer.readableBytes();		
		final ChannelFuture cf;
		if(codec==TraceCodec.JSON) {
			final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/put?details", sendBuffer);
			request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
			request.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes);
			if(gzipEnabled) {
				request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
			}
			cf = channel.writeAndFlush(request);		
		} else {
			cf = channel.writeAndFlush(sendBuffer);
		}
		cf.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(final ChannelFuture f) throws Exception {
				if(f.isSuccess()) {
					sendCounter.inc();
//					log.info("Flushed {} Bytes", bytes);
					//datapointMeter.mark(datapoints);
					//totalBytesSent.add(bytes);
					if(metricsEnabled.get()) {
						bytesSent.update(bytes);
					}
				} else {
					traceErrorCounter.inc();
					log.error("TraceSend Failed", f.cause());
				}
			} 
		});
		return cf;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#traceCodec()
	 */
	@Override
	public TraceCodec traceCodec() {		
		return codec;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(io.netty.buffer.ByteBuf)
	 */
	@Override
	public void onResponse(final ByteBuf response) {
		dropLatch();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(io.netty.channel.socket.DatagramPacket)
	 */
	@Override
	public void onResponse(final DatagramPacket response) {
		dropLatch();
	}
	
	
	protected void dropLatch() {
		final CountDownLatch latch = responseLatch.getAndSet(null);
		if(latch!=null) latch.countDown();
	}
	
	protected void waitOnLatch(final long timeoutMs) {
		final CountDownLatch latch = responseLatch.get();
		if(latch!=null) {
			try {
				if(!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
					throw new RuntimeException("Thread timed out while waiting on flush latch (" + timeoutMs + ") ms.");
				}
			} catch (InterruptedException iex) {
				throw new RuntimeException("Thread interrupted while waiting on flush latch", iex);
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(io.netty.handler.codec.http.HttpObject)
	 */
	@Override
	public void onResponse(final HttpObject response) {
		dropLatch();
		if(metricsEnabled.get()) {
			if(response instanceof FullHttpResponse) {
				final FullHttpResponse full = (FullHttpResponse)response;
				final ByteBuf buf = full.content();
				buf.retain();
	//			log.info("Response: {}", buf.toString(UTF8));
				group.execute(new Runnable(){
					public void run() {
						final JsonNode node = JSONOps.parseToNode(buf);
						if(node.has("elapsed")) {
							final long elapsed = node.get("elapsed").asLong();
							importTimer.update(elapsed, TimeUnit.NANOSECONDS);
							importAlloc.update(node.get("allocated").asLong());
						}
					}
				});
				
	//			log.info("Response: {}", full.content().toString(UTF8));
			} else {
	//			log.info("Not a full response: {}", response);
			}
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(java.lang.String[])
	 */
	@Override
	public void onResponse(final String... response) {
		dropLatch();
		log.debug("Response: [{}]", response[0]);
		if(metricsEnabled.get()) {
			group.execute(new Runnable(){
				public void run() {
					final JsonNode node = JSONOps.parseToNode(response[0]);
					if(node.has("cause")) {
						log.error("Trace failure:[{}]", node.get("cause").textValue());
						traceErrorCounter.inc();
					} else {
						final long elapsed = node.get("elapsed").asLong();
						importTimer.update(elapsed, TimeUnit.NANOSECONDS);
						importAlloc.update(node.get("allocated").asLong());
						traceErrorCounter.inc(node.get("errors").asLong());
					}
				}
			});
		}
	}

	
	public long getTotalDatapointsSent() {
		return totalDatapointsSent.longValue();
	}
	
	public long getCurrentBatchSize() {
		return currentBatchSize.longValue();
	}

	public MetricRegistry getRegistry() {
		return registry;
	}

	public ClientConfiguration getClientConfig() {
		return clientConfig;
	}
	
	public BaseClient<T> custom(final String key, final Object value) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		if(value==null) throw new IllegalArgumentException("The passed value was null");
		clientConfig.putCustom(key, value);
		return this;
	}
	

}
