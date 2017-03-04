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
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

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
import net.opentsdb.client.CallbackHandler;
import net.opentsdb.client.ClientConfiguration;
import net.opentsdb.client.Protocol;
import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.tracing.TraceCodec;
import net.opentsdb.client.tracing.Tracer;
import net.opentsdb.client.util.SpinLock;

/**
 * <p>Title: BaseClient</p>
 * <p>Description: The abstract base client</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.protocol.BaseClient</code></p>
 */

abstract class BaseClient implements Tracer, CallbackHandler, MetricSet {
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
	
	protected final LongAdder totalBytesSent = new LongAdder();
	protected final LongAdder totalDatapointsSent = new LongAdder();
	protected final LongAdder currentBatchSize = new LongAdder();
	
	protected final MetricRegistry registry = new MetricRegistry();
	protected final Timer traceFlushTimer = registry.timer("flush");
	protected final Meter datapointMeter = registry.meter("datapoints");
	
	protected final JmxReporter reporter = JmxReporter.forRegistry(registry)
			.registerWith(ManagementFactory.getPlatformMBeanServer())
			.build();
	
	protected final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(registry).build();
	
	
	protected Bootstrap bootstrap = new Bootstrap();
	protected EventLoopGroup group = null;
	protected ChannelInitializer<? extends Channel> initializer = null;
	protected Class<? extends Channel> channelClass = null;
	protected Channel channel = null;
	
	protected final Map<String, Metric> registeredMetrics;
	protected final AtomicBoolean metricsEnabled = new AtomicBoolean(false);
	
	protected final AtomicReference<CountDownLatch> responseLatch = new AtomicReference<CountDownLatch>(null);
	

	/**
	 * Creates a new BaseClient
	 * @param msTime Indicates if we're using ms timestamps (true) or second timestamps (false)
	 * @param directBuffers Indicates if we're using direct allocation byte buffers (true) or heap buffers (false)
	 * @param pooledBuffers Indicates if we're using pooled buffers (true) or unpooled buffers (false)
	 * @param codec The data point encoding
	 */
	protected BaseClient(final ClientConfiguration clientConfiguration) {
		if(clientConfiguration==null) throw new IllegalArgumentException("The passed ClientConfiguration was null");
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
		swap();
		group = protocol.eventLoopGroup(clientConfiguration);
		initializer = protocol.channelInitializer(clientConfiguration, this);
		channelClass = protocol.channelClass(clientConfiguration);
		bootstrap
			.channel(channelClass)
			.group(group)
			.handler(initializer);
		registeredMetrics = new HashMap<String, Metric>(registry.getMetrics()); 
		channel =  bootstrap.connect(socketAddress).syncUninterruptibly().channel();
		reporter.start();
	}
	
	public void close() {
		try { channel.close().sync(); } catch (Exception x) {/* No Op */}
		try { group.shutdownGracefully(); } catch (Exception x) {/* No Op */}
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
	
	/**
	 * Swaps out the current trace buffer and output stream with a new one.
	 * @return the prior trace buffer or null if one did not exist.
	 * @throws Exception
	 */
	protected ByteBuf swap() {
		return spinLock.doInLock(true, new Callable<ByteBuf>(){
			@Override
			public ByteBuf call() throws Exception {
				final ByteBuf buff = allocator.buffer(initialTraceBufferSize);
				final ByteBufOutputStream buffOut = new ByteBufOutputStream(buff);
				ByteBuf retBuf = null;
				if(output!=null) {
					try { 
						codec.tailer(output);
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
				codec.header(output);
				return retBuf;
			}
		});
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(long, java.lang.String, long, java.util.Map)
	 */
	@Override
	public void trace(final long time, final String metric, final long value, final Map<String, String> tags) {
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				codec.encode(output, time, metric, value, tags);
			}
		});
		currentBatchSize.increment();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(long, java.lang.String, double, java.util.Map)
	 */
	@Override
	public void trace(final long time, final String metric, final double value, final Map<String, String> tags) {
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				codec.encode(output, time, metric, value, tags);
			}
		});
		currentBatchSize.increment();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(java.lang.String, long, java.util.Map)
	 */
	@Override
	public void trace(final String metric, final long value, final Map<String, String> tags) {
		trace(time(), metric, value, tags);		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#trace(java.lang.String, double, java.util.Map)
	 */
	@Override
	public void trace(final String metric, final double value, final Map<String, String> tags) {
		trace(time(), metric, value, tags);		
	}
	
	/**
	 * Returns the current time in ms. or s. depending on config
	 * @return the current time
	 */
	protected long time() {
		return conversion.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Tracer#flush()
	 */
	@Override
	public void flush() {
		final long dpoints = currentBatchSize.sumThenReset();
		final Context ctx = traceFlushTimer.time();
		final ByteBuf flushedBuffer = swap();
		if(flushedBuffer!=null && flushedBuffer.readableBytes() > 0) {
			final CountDownLatch latch = new CountDownLatch(1);
			responseLatch.set(latch);
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
			try {
				if(!latch.await(15000, TimeUnit.SECONDS)) {
					throw new RuntimeException("Timeout while waiting on response");
				}
			} catch (InterruptedException ex) {
				throw new RuntimeException("Thread interrupted while waiting on response");
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
	
	/**
	 * @param sendBuffer
	 * @return
	 */
	protected ChannelFuture send(final ByteBuf sendBuffer) {
		final long bytes = sendBuffer.readableBytes();
		final long datapoints = getCurrentBatchSize();
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
//					log.info("Flushed {} Bytes", bytes);
					//datapointMeter.mark(datapoints);
					totalBytesSent.add(bytes);
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
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(io.netty.handler.codec.http.HttpObject)
	 */
	@Override
	public void onResponse(final HttpObject response) {
		dropLatch();
		if(response instanceof FullHttpResponse) {
			final FullHttpResponse full = (FullHttpResponse)response;
//			log.info("Response: {}", full.content().toString(UTF8));
		} else {
//			log.info("Not a full response: {}", response);
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.CallbackHandler#onResponse(java.lang.String[])
	 */
	@Override
	public void onResponse(final String... response) {
		dropLatch();
//		log.info("Response: {}", response[0]);
	}

	public long getTotalBytesSent() {
		return totalBytesSent.longValue();
	}
	
	public long getTotalDatapointsSent() {
		return totalDatapointsSent.longValue();
	}
	
	public long getCurrentBatchSize() {
		return currentBatchSize.longValue();
	}
	

}
