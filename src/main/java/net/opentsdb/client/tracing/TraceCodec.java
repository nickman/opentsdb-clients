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
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.http.HttpObject;
import net.opentsdb.client.CallbackHandler;
import net.opentsdb.client.json.JSONOps;


/**
 * <p>Title: TraceEncoding</p>
 * <p>Description: Enumerates the encoding used when sending datapoints</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.tracing.TraceCodec</code></p>
 */

public enum TraceCodec implements Codec {
	/** Telnet put command encoding */
	PUT(new TelnetEncoder()),
	/** Import command encoding */
	TEXT(new TextEncoder()),
	/** JSON encoding */
	JSON(new JSONEncoder()),
	/** JSON text encoding */
	JSONTEXT(new JSONTextEncoder());
	
	
	private TraceCodec(final Codec encoder) {
		this.encoder = encoder;
	}
	
	private final Codec encoder;
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Codec#encode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
	 */
	@Override
	public void encode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
		encoder.encode(out, time, metric, value, tags);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Codec#encode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
	 */
	@Override
	public void encode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
		encoder.encode(out, time, metric, value, tags);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Codec#header(java.io.OutputStream)
	 */
	@Override
	public void header(final OutputStream out) {
		encoder.header(out);		
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Codec#tailer(java.io.OutputStream)
	 */
	@Override
	public void tailer(final OutputStream out) {
		encoder.tailer(out);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.client.tracing.Codec#outboundHandler(net.opentsdb.client.CallbackHandler)
	 */
	@Override
	public <T> SimpleChannelInboundHandler<T> outboundHandler(final CallbackHandler callbackHandler) {
		return encoder.outboundHandler(callbackHandler);
	}
	
	public static abstract class BaseEncoder implements Codec {
		/** UTF8 bytes for a space */
		private static final byte[] SPACE = " ".getBytes(UTF8);
		/** UTF8 bytes for an equals  */
		private static final byte[] EQ = "=".getBytes(UTF8);
		/** UTF8 bytes for a PUT  */
		private static final byte[] PUTBYTES = "put ".getBytes(UTF8);
		/** UTF8 bytes for an EOL  */
		private static final byte[] EOL = "\n".getBytes(UTF8);
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#header(java.io.OutputStream)
		 */
		@Override
		public void header(final OutputStream out) {
			/* No Op */			
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#tailer(java.io.OutputStream)
		 */
		@Override
		public void tailer(final OutputStream out) {
			/* No Op */
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#encode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
		 */
		@Override
		public void encode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
			if(out==null) throw new IllegalArgumentException("The passed OutputStream was null");
			if(metric==null || metric.trim().isEmpty()) throw new IllegalArgumentException("The passed metric was null or empty");
			if(tags==null || tags.isEmpty()) throw new IllegalArgumentException("The passed tag map was null or empty");
			doEncode(out, time, metric, value, tags);
		}
		
		/**
		 * Encodes the passed data point
		 * @param buffer The OutputStream to write the encoded data point into
	     * @param time The timestamp of the metric
	     * @param metric The metric name
	     * @param value The value
	     * @param tags The metric tags
		 */
		protected abstract void doEncode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags);

		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#encode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
		 */
		@Override
		public void encode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
			if(out==null) throw new IllegalArgumentException("The passed OutputStream was null");
			if(metric==null || metric.trim().isEmpty()) throw new IllegalArgumentException("The passed metric was null or empty");
			if(tags==null || tags.isEmpty()) throw new IllegalArgumentException("The passed tag map was null or empty");
			doEncode(out, time, metric, value, tags);			
		}
		
		/**
		 * Encodes the passed data point
		 * @param buffer The OutputStream to write the encoded data point into
	     * @param time The timestamp of the metric
	     * @param metric The metric name
	     * @param value The value
	     * @param tags The metric tags
		 */
		protected abstract void doEncode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags);
		
		/**
		 * Writes an EOL to the passed output stream
		 * @param out The output stream to write to 
		 */
		protected void writeEOL(final OutputStream out) {
			try {				
				out.write(EOL);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write EOL to OutputStream", ex);
			}			
		}
		
		/**
		 * Writes the passed string to the output stream
		 * @param out The output stream to write to 
		 * @param value The value to write
		 * @param leadingSpace true for a leading space
		 */
		protected void write(final OutputStream out, final String value, final boolean leadingSpace) {
			try {
				if(leadingSpace) out.write(SPACE);
				out.write(value.trim().getBytes(UTF8));
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write value [" + value + "] to OutputStream", ex);
			}
		}
		
		/**
		 * Writes the passed map to the output stream
		 * @param out The output stream to write to 
		 * @param map The map to write
		 * @param leadingSpace true for a leading space
		 */
		protected void write(final OutputStream out, final Map<String, String> map, final boolean leadingSpace) {
			try {
				if(leadingSpace) out.write(SPACE);
				for(Map.Entry<String, String> entry: map.entrySet()) {
					out.write(SPACE);
					out.write(entry.getKey().trim().getBytes(UTF8));
					out.write(EQ);
					out.write(entry.getValue().trim().getBytes(UTF8));
				}
				
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write map [" + map + "] to OutputStream", ex);
			}
		}
		
		/**
		 * Writes a telnet put to the output stream
		 * @param out The output stream to write to 
		 */
		protected void writePut(final OutputStream out) {
			try {
				out.write(PUTBYTES);
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write PUT to OutputStream", ex);
			}
		}
		
		
		
	}
	
	
	/**
	 * <p>Title: TextEncoder</p>
	 * <p>Description: Text data point encoder.</p>
	 * <p>Format: <b><code>sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0</code></b> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.client.tracing.TraceEncoding.TextEncoder</code></p>
	 */
	public static class TextEncoder extends BaseEncoder {

		@SuppressWarnings("unchecked")
		@Override
		public SimpleChannelInboundHandler<String> outboundHandler(final CallbackHandler callbackHandler) {			
			return new SimpleChannelInboundHandler<String>() {
				@Override
				protected void channelRead0(final ChannelHandlerContext ctx, final String msg) throws Exception {
					callbackHandler.onResponse(msg);
				}
				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					if(msg instanceof DatagramPacket) {
						callbackHandler.onResponse((DatagramPacket)msg);
					} else {
						callbackHandler.onResponse(msg.toString());
					}					
				}
			};
		}
		
		@Override
		public void header(final OutputStream out) {
			write(out, "PUTBATCH --send-response", false);
			writeEOL(out);
		}
		
		@Override
		public void tailer(final OutputStream out) {
			writeEOL(out);
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
			write(out, metric, false);
			write(out, Long.toString(time), true);
			write(out, Long.toString(value), true);
			write(out, tags, false);
			writeEOL(out);
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
			write(out, metric, false);
			write(out, Long.toString(time), true);
			write(out, Double.toString(value), true);
			write(out, tags, false);
			writeEOL(out);
		}
	}
	
	
	/**
	 * <p>Title: TelnetEncoder</p>
	 * <p>Description: Telnet put command data point encoder.</p>
	 * <p>Format: <b><code>put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0</code></b> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.client.tracing.TraceEncoding.TelnetEncoder</code></p>
	 */
	public static class TelnetEncoder extends TextEncoder {
		
		@SuppressWarnings("unchecked")
		@Override
		public SimpleChannelInboundHandler<String> outboundHandler(final CallbackHandler callbackHandler) {			
			return new SimpleChannelInboundHandler<String>() {
				@Override
				protected void channelRead0(final ChannelHandlerContext ctx, final String msg) throws Exception {
					callbackHandler.onResponse(msg);
				}
			};
		}
		
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
			writePut(out);
			super.doEncode(out, time, metric, value, tags);
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
			writePut(out);
			super.doEncode(out, time, metric, value, tags);			
		}
	}
	
	public static class JSONTextEncoder extends BaseEncoder {

		@SuppressWarnings("unchecked")
		@Override
		public SimpleChannelInboundHandler<String> outboundHandler(final CallbackHandler callbackHandler) {			
			return new SimpleChannelInboundHandler<String>() {
				@Override
				protected void channelRead0(final ChannelHandlerContext ctx, final String msg) throws Exception {
					callbackHandler.onResponse(msg);
				}
				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					callbackHandler.onResponse(msg.toString());
				}
			};
		}
		
		@Override
		public void header(final OutputStream out) {
			write(out, "putbatchjson", false);
			writeEOL(out);
			try {
				JSONOps.generatorFor(out).writeStartArray();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}			
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#tailer(java.io.OutputStream)
		 */
		@Override
		public void tailer(final OutputStream out) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeEndArray();
				jgen.flush();
				jgen.close();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}
		}


		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeStartObject();
				jgen.writeStringField("metric", metric.trim());
				jgen.writeNumberField("timestamp", time);
				jgen.writeNumberField("value", value);
				jgen.writeFieldName("tags");
				jgen.writeStartObject();
				for(Map.Entry<String, String> entry: tags.entrySet()) {
					jgen.writeStringField(entry.getKey().trim(), entry.getValue().trim());
				}
				jgen.writeEndObject();
				jgen.writeEndObject();
				jgen.flush();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeStartObject();
				jgen.writeStringField("metric", metric.trim());
				jgen.writeNumberField("timestamp", time);
				jgen.writeNumberField("value", value);
				jgen.writeFieldName("tags");
				jgen.writeStartObject();
				for(Map.Entry<String, String> entry: tags.entrySet()) {
					jgen.writeStringField(entry.getKey().trim(), entry.getValue().trim());
				}
				jgen.writeEndObject();
				jgen.writeEndObject();
				jgen.flush();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}			
		}

		

		
		
		
	}
	/**
	 * <p>Title: TelnetEncoder</p>
	 * <p>Description: Telnet put command data point encoder.</p>
	 * <p>Format: <b><pre>
	 * {
	 *     "metric": "sys.cpu.nice",
	 *     "timestamp": 1346846400,
	 *     "value": 18,
	 *     "tags": {
	 *        "host": "web01",
	 *        "dc": "lga"
	 *     }
	 * }
	 *  </pre></b></p>
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.client.tracing.TraceEncoding.TelnetEncoder</code></p>
	 */
	public static class JSONEncoder extends BaseEncoder {
		
		@SuppressWarnings("unchecked")
		@Override
		public SimpleChannelInboundHandler<HttpObject> outboundHandler(final CallbackHandler callbackHandler) {			
			return new SimpleChannelInboundHandler<HttpObject>() {
				@Override
				protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
					callbackHandler.onResponse(msg);
				}
			};
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#header(java.io.OutputStream)
		 */
		@Override
		public void header(final OutputStream out) {
			try {
				JSONOps.generatorFor(out).writeStartArray();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.Codec#tailer(java.io.OutputStream)
		 */
		@Override
		public void tailer(final OutputStream out) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeEndArray();
				jgen.flush();
				jgen.close();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}
		}

		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, long, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final long value, final Map<String, String> tags) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeStartObject();
				jgen.writeStringField("metric", metric.trim());
				jgen.writeNumberField("timestamp", time);
				jgen.writeNumberField("value", value);
				jgen.writeFieldName("tags");
				jgen.writeStartObject();
				for(Map.Entry<String, String> entry: tags.entrySet()) {
					jgen.writeStringField(entry.getKey().trim(), entry.getValue().trim());
				}
				jgen.writeEndObject();
				jgen.writeEndObject();
				jgen.flush();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}
		}
		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.client.tracing.TraceCodec.BaseEncoder#doEncode(java.io.OutputStream, long, java.lang.String, double, java.util.Map)
		 */
		@Override
		protected void doEncode(final OutputStream out, final long time, final String metric, final double value, final Map<String, String> tags) {
			final JsonGenerator jgen = JSONOps.generatorFor(out);
			try {
				jgen.writeStartObject();
				jgen.writeStringField("metric", metric.trim());
				jgen.writeNumberField("timestamp", time);
				jgen.writeNumberField("value", value);
				jgen.writeFieldName("tags");
				jgen.writeStartObject();
				for(Map.Entry<String, String> entry: tags.entrySet()) {
					jgen.writeStringField(entry.getKey().trim(), entry.getValue().trim());
				}
				jgen.writeEndObject();
				jgen.writeEndObject();
				jgen.flush();
			} catch (Exception ex) {
				throw new RuntimeException("Failed to write json to output stream", ex);
			}			
		}
	}
	
	
	
}
