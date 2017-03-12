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
package net.opentsdb.client.util;

import java.nio.charset.Charset;
import java.util.Iterator;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

/**
 * <p>Title: ByteBufStringSplitter</p>
 * <p>Description: Splits a ByteBuf by the specified ByteProcessor returning the split item as a string.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.util.ByteBufStringSplitter</code></p>
 */

public class ByteBufStringSplitter implements Iterable<String>, Iterator<String> {
	
	private static final Charset UTF8 = Charset.forName("UTF8");
	
	/** The character set to read strings as */
	protected Charset charset;
	/** The copy or reference option */
	protected final boolean copy;
	/** The buf splitter */
	protected final ByteBufSplitter splitter;
	
	public ByteBufStringSplitter(final ByteBuf buf, final ByteProcessor processor, final boolean copy, final Charset charset) {
		this.charset = charset==null ? UTF8 : charset;
		splitter = new ByteBufSplitter(buf, copy, processor);
		this.copy = copy;
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {		
		return splitter.hasNext();
	}

	/**
	 * {@inheritDoc}
	 * @see java.util.Iterator#next()
	 */
	@Override
	public String next() {
		final ByteBuf sbuf = splitter.next(); 
		final String s = sbuf.toString(charset);
		if(copy) ReferenceCountUtil.safeRelease(sbuf);
		return s;
	}

	/**
	 * <p>Not implemented</p>
	 * {@inheritDoc}
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		/* No Op */		
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<String> iterator() {
		return this;
	}
	
	

}
