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
package net.opentsdb.client.protocol.udp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;
import net.opentsdb.client.buffer.BufferManager;

/**
 * <p>Title: UDPPacketHandler</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.protocol.udp.TraceOutputHandler</code></p>
 */

public class TraceOutputHandler extends MessageToMessageEncoder<ByteBuf> {
	
	protected final boolean gzip;
	protected final int maxMessageSize;
	protected final BufferManager bufferManager = BufferManager.getInstance();
	
	
	public TraceOutputHandler(final boolean gzip, final int maxMessageSize) {
		this.gzip = gzip;
		this.maxMessageSize = maxMessageSize;
	}

	public static void main (String[] args) {
		final TraceOutputHandler toh = new TraceOutputHandler(true, 24);
		final List<Object> out = new ArrayList<Object>();
		final ByteBuf buf = toh.bufferManager.wrap("HELLO\nThe moon is a ballon\nIt's not the men in your life, but the life in your men\n", UTF8);
		try {
			toh.encode(null, buf, out);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
		
	}
	
	@Override
	protected void encode(final ChannelHandlerContext ctx, final ByteBuf msg, final List<Object> out) throws Exception {
		final int payloadStarts = msg.forEachByte(ByteProcessor.FIND_CRLF) + 1;
		final ByteBuf command = msg.copy(msg.readerIndex(), payloadStarts).asReadOnly();
		msg.readerIndex(payloadStarts);
		while(true) {
			final ByteBuf outBuf = bufferManager.buffer(maxMessageSize * 2);
			final int index = outBuf.writerIndex();
			final int bytesCopied = payload(msg, outBuf, command);
			if(bytesCopied > index) {
				out.add(outBuf);
			} else {
				ReferenceCountUtil.safeRelease(outBuf);
				break;
			}
		}
		ReferenceCountUtil.safeRelease(msg);		
	}
	
	
	protected int payload(final ByteBuf inBuf, final ByteBuf outBuf, final ByteBuf command) throws Exception {
		int startingIndex = inBuf.readerIndex();
		int toIndex = -1;
		int inRewind = startingIndex;
		int outRewind = outBuf.writerIndex();
		final ByteBufOutputStream bbos = new ByteBufOutputStream(outBuf);			
		final GZIPOutputStream gos;
		final OutputStream os;
		
		if(gzip) {
			gos = new GZIPOutputStream(bbos);
			os = gos;
		} else {
			gos = null;
			os = bbos;
		}
		try {
			os.write(ByteBufUtil.getBytes(command));
			while((toIndex = inBuf.forEachByte(startingIndex, inBuf.readableBytes() - startingIndex, ByteProcessor.FIND_CRLF))!=-1) {
//				toIndex++;
				inBuf.readBytes(os, toIndex - startingIndex);
				os.flush();
				bbos.flush();
				if(outBuf.writerIndex() > maxMessageSize) {
					inBuf.readerIndex(inRewind);
					outBuf.writerIndex(outRewind);
					break;
				} else {
					inRewind = inBuf.readerIndex();
					outRewind = outBuf.writerIndex();
					startingIndex = toIndex;
				}
			}
			return outBuf.writerIndex();
		} finally {
			if(gzip) {
				try { gos.finish(); } catch (Exception x) {/* No Op */}
				try { gos.close(); } catch (Exception x) {/* No Op */}
			}
			try { bbos.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	private static final Charset UTF8 = Charset.forName("UTF8");
	
	public static String fromZippedBuf(final ByteBuf buf) {
		ByteBufInputStream bbis = new ByteBufInputStream(buf.slice());
		GZIPInputStream gis = null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(buf.readableBytes() * 10);
		final byte[] buffer = new byte[1024];
		int bytesRead = -1;
		try {
			gis = new GZIPInputStream(bbis);
			while((bytesRead = gis.read(buffer))!=-1) {
				baos.write(buffer, 0, bytesRead);
			}
			baos.flush();
			return new String(baos.toByteArray(), UTF8);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			try { gis.close(); } catch (Exception x) {}
			try { bbis.close(); } catch (Exception x) {}
			try { baos.close(); } catch (Exception x) {}
		}
	}

}
