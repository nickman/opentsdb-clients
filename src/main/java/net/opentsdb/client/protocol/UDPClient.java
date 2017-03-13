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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.databind.JsonNode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;
import net.opentsdb.client.ClientConfiguration;
import net.opentsdb.client.ClientFactory;
import net.opentsdb.client.buffer.BufferManager;
import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.util.ByteBufStringSplitter;

/**
 * <p>Title: UDPClient</p>
 * <p>Description: The UDP protocol client</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.protocol.UDPClient</code></p>
 */

public class UDPClient extends BaseClient<UDPClient> {
	/** The tracer buffer's gzip OutputStream */
	protected GZIPOutputStream gzipOutput = null;
	
	protected final int MAX_OUTPUT = 2048;
	protected final int FLUSH_ON = MAX_OUTPUT - 3;
	protected final int PRE_FLUSH = 128;
	private static final String STATS = "STATS:";
	private final AtomicLong statsCounter = new AtomicLong();
	private final AtomicLong responseCounter = new AtomicLong();
	private final InetSocketAddress isa;
	private final InetSocketAddress localIsa;
	
	
	
	
	protected final BlockingQueue<Runnable> responseQueue = new ArrayBlockingQueue<Runnable>(1024, true);
	protected final AtomicBoolean keepRunning = new AtomicBoolean(true);
	protected final Thread responseQueueThread = new Thread("UDPClientResponseQueueProcessor") {
		@Override
		public void run() {			
			while(keepRunning.get()) {
				try {
					final Runnable r = responseQueue.take();
					r.run();
				} catch (InterruptedException iex) {
					if(Thread.interrupted()) Thread.interrupted();
				} catch (Exception ex) {
					log.error("Failed to process response", ex);
				}
			}
		}
	};

	/**
	 * Creates a new UDPClient
	 * @param clientConfiguration
	 */
	public UDPClient(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
		//channel.pipeline().addFirst("logger", new LoggingHandler(getClass(), LogLevel.INFO));
		isa = (InetSocketAddress)socketAddress;
		localIsa = (InetSocketAddress)channel.localAddress();
		responseQueueThread.setDaemon(true);
		responseQueueThread.start();
	}
	
	@Override
	public void close() {				
		keepRunning.set(false);
		responseQueueThread.interrupt();
		super.close();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.protocol.BaseClient#writeHeader()
	 */
	@Override
	protected void writeHeader() {
		if(gzipEnabled) {
			if(gzipOutput==null) {
				try {
					gzipOutput = new GZIPOutputStream(output, MAX_OUTPUT, true);
					codec.header(gzipOutput);
				} catch (Exception ex) {
					throw new RuntimeException("Failed to initialize header with gzipped out", ex);
				}
			}
		} else {
			codec.header(output);
		}		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.protocol.BaseClient#writeTailer()
	 */
	@Override
	protected void writeTailer() {
		if(gzipEnabled) {
			codec.tailer(gzipOutput);
			try {
				gzipOutput.flush();
				gzipOutput.finish();
				gzipOutput = null;
			} catch (Exception ex) {
				throw new RuntimeException("Failed to flush zip output", ex);
			}
		} else {
			codec.tailer(output);
		}			
	}

	
	protected byte[] preEncode(final long time, final String metric, final long value, final Map<String, String> tags) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(PRE_FLUSH); 
		try {
			codec.encode(baos, time, metric, value, tags);
			baos.flush();
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to pre-encode long value", ex);
		} finally {
			try { baos.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	protected byte[] preEncode(final long time, final String metric, final double value, final Map<String, String> tags) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(PRE_FLUSH); 
		try {
			codec.encode(baos, time, metric, value, tags);
			baos.flush();
			return baos.toByteArray();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to pre-encode double value", ex);
		} finally {
			try { baos.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.protocol.BaseClient#trace(long, java.lang.Object, long, java.util.Map)
	 */
	@Override
	public void trace(final long time, final Object metric, final long value, final Map<Object, Object> tags) {
		final byte[] encoded;
		try {
			encoded = preEncode(time, cleanMetric(metric), value, cleanTags(tags));
		} catch (IllegalArgumentException iae) {
			invalidTraceCounter.inc();
			log.debug("Invalid trace: {}:{}", metric, tags);			
			return;
		}
		if(output.writtenBytes() + encoded.length > FLUSH_ON) {
			flush();
		}
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				try {
					if(gzipEnabled) {
						gzipOutput.write(encoded);
						gzipOutput.flush();
					} else {
						output.write(encoded);
					}
					totalDatapointsSent.increment();
				} catch (Exception ex) {
					throw new RuntimeException("Failed to write pre-encode long value", ex);
				}
			}
		});
		bytesEncoded.add(encoded.length);
		currentBatchSize.increment();
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.protocol.BaseClient#trace(long, java.lang.Object, double, java.util.Map)
	 */
	@Override
	public void trace(final long time, final Object metric, final double value, final Map<Object, Object> tags) {
		final byte[] encoded;
		try {
			encoded = preEncode(time, cleanMetric(metric), value, cleanTags(tags));
		} catch (IllegalArgumentException iae) {
			invalidTraceCounter.inc();
			log.debug("Invalid trace: {}:{}", metric, tags);			
			return;
		}
		if(output.writtenBytes() + encoded.length > FLUSH_ON) {
			flush();
		}
		spinLock.doInLock(new Runnable(){
			@Override
			public void run() {
				try {
					if(gzipEnabled) {
						gzipOutput.write(encoded);
						gzipOutput.flush();
					} else {
						output.write(encoded);
					}
					totalDatapointsSent.increment();
				} catch (Exception ex) {
					throw new RuntimeException("Failed to write pre-encode double value", ex);
				}
			}
		});
		bytesEncoded.add(encoded.length);
		currentBatchSize.increment();
	}
	
	/**
	 * Sends the accumulated payload
	 * @param sendBuffer The accumulated buffer
	 * @return The write's ChannelFuture
	 */
	protected ChannelFuture send(final ByteBuf sendBuffer) {
		DatagramPacket dp = new DatagramPacket(sendBuffer, isa);
		final ChannelFuture cf = channel.writeAndFlush(dp);
		cf.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(final ChannelFuture f) throws Exception {				
				if(!f.isSuccess()) {
					throw new RuntimeException("Failed to send UDP datagram", f.cause());
				} else {
					sendCounter.inc();
					bytesSent.update(bytesEncoded.sumThenReset());
				}
			}
		});
		return cf;
	}
	
//	@Override
//	public void onResponse(String... response) {
//		System.err.println("Response:[" + response[0] + "]");
//		
//	}
	
	public static void main(String[] args) {
		final File clientConfig = new File("./src/test/resources/configs/udp-text.conf");
		final UDPClient client = ClientFactory.client(clientConfig);
//		client.trace("super", Math.PI, Collections.singletonMap("symbol", "pi"));
//		client.flush();
		while(true) {
			client.requestStats();
			try { Thread.currentThread().join(5000); } catch (Exception x) {/* No Op */}
		}
//		client.close();
		
	}
	
	
	
	public void requestStats() {
		channel.writeAndFlush(new DatagramPacket(BufferManager.getInstance().wrap("STATS\n"), isa, localIsa));
		log.debug("Sent Stats Request to [{}]", isa);
	}
	
	@Override
	public void onResponse(final DatagramPacket response) {
		dropLatch();
//		response.retain();
		responseQueue.add(new Runnable(){
			public void run() {
				
				final ByteBuf buf = response.content();
				final Command cmd = Command.extractCommand(buf);
//				log.info("Responses: {}, this one: {}", responseCounter.incrementAndGet(), cmd.name());
				switch(cmd) {
				case PUTBATCH:
					
					final ByteBuf payload = Command.extractPayload(buf);
					final JsonNode node = JSONOps.parseToNode(payload.toString(UTF8));
//					log.info("putbatch response: [{}]", node);
					if(node.has("cause")) {
						log.error("Trace failure:[{}]", node.get("cause").textValue());
						traceErrorCounter.inc();
					} else {
						if(metricsEnabled.get()) {
							final long elapsed = node.get("elapsed").asLong();
							importTimer.update(elapsed, TimeUnit.NANOSECONDS);
							importAlloc.update(node.get("allocated").asLong());
							traceErrorCounter.inc(node.get("errors").asLong());
						}
					}
					break;
				case STATS:
					onStats(buf);
					break;
				default:
					log.warn("Unimplemented command handler for [{}]", cmd.name());				
				}
				//ReferenceCountUtil.safeRelease(response);
			}
		});
		
		 
		
//		System.err.println("Response:[" + response.content().toString(UTF8) + "]");
		
	}
	
	
	protected void onStats(final ByteBuf buf) {		
		final ByteBufStringSplitter splitter = new ByteBufStringSplitter(buf, ByteProcessor.FIND_CRLF, false, UTF8);
		int tracedCount = 0;
		while(splitter.hasNext()) {			
			try {
				final String s = splitter.next();
				if(STATS.equals(s)) continue;				
				trace(s);
				tracedCount++;
//				log.info("------------> Stat: [{}]", s);
//				log.info("Processed Stats: {}", statsCounter.incrementAndGet());
			} catch (Exception ex) {
				log.error("Stat trace error", ex);
			}
		}
		log.debug("Traced [{}] Stats", tracedCount);
		flush();
		
	}
	
	
//	protected ByteBuf swap() {
//		return spinLock.doInLock(new Callable<ByteBuf>(){
//			@Override
//			public ByteBuf call() throws Exception {
//				final ByteBuf swapped = UDPClient.super.swap();
//				if(gzipEnabled) {
//					gzipOutput = new GZIPOutputStream(output, MAX_OUTPUT, true);
//				}
//				return swapped;
//			}
//		});
//	}
	

}
