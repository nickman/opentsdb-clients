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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.ByteProcessor.IndexOfProcessor;
import net.opentsdb.client.buffer.BufferManager;
import net.opentsdb.client.util.StringsUtil;

/**
 * <p>Title: Command</p>
 * <p>Description: Enumerates the commands that can be issued through a client</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.protocol.Command</code></p>
 */

public enum Command implements ICommand {
	PUTBATCH(" --send-response\n", null, new String[]{"--request-id"}, new String[]{"--skip-errors", "--send-response"}),	
	STATS(" \n", null, new String[]{"--request-id"}, new String[]{"--canonical"});
	

	
	private Command(final String request, final String[] subCommands, final String[] options, final String[] flags) {
		this.request = BufferManager.getInstance().upwrap(name() + request).asReadOnly();
		if(subCommands!=null && subCommands.length > 0) {
			final Set<String> tmp = new HashSet<String>(subCommands.length);
			Collections.addAll(tmp, subCommands);
			this.subCommands = Collections.unmodifiableSet(tmp);				
		} else {
			this.subCommands = EMPTY_SET;
		}
		
		if(options!=null && options.length > 0) {
			final Set<String> tmp = new HashSet<String>(options.length);
			Collections.addAll(tmp, options);
			this.options = Collections.unmodifiableSet(tmp);				
		} else {
			this.options = EMPTY_SET;
		}
		if(flags!=null && flags.length > 0) {
			final Set<String> tmp = new HashSet<String>(flags.length);
			Collections.addAll(tmp, flags);
			this.flags = Collections.unmodifiableSet(tmp);				
		} else {
			this.flags = EMPTY_SET;
		}			
	}
	
	public final Set<String> subCommands;
	public final Set<String> flags;
	public final Set<String> options;
	public final ByteBuf request;
	
	/** Aborts on a {@code CR (':')} */
	public static final ByteProcessor FIND_COLON = new IndexOfProcessor((byte) ':');
	
	
	public static void main(String[] args) {
		final StringBuilder b = new StringBuilder("PUTBATCH:\n")
			.append("Hello\nWorld\n");
		final ByteBuf buf = BufferManager.getInstance().wrap(b);
		System.out.println("Command:" + extractCommand(buf));
		System.out.println("Payload:\n[" + extractPayload(buf).toString(UTF8) + "]");
	}
	
	public static Command extractCommand(final ByteBuf buf) {
		if(buf==null) throw new IllegalArgumentException("The passed buffer was null");
		if(!buf.isReadable()) throw new IllegalArgumentException("The passed buffer was not readable [" + buf + "]");
		final int index = buf.forEachByte(FIND_COLON);
		if(index==-1) throw new RuntimeException("Failed to find response type code");
		final String cmd = buf.slice(0, index).toString(UTF8).toUpperCase().trim();
		final String[] cmds = StringsUtil.splitString(cmd, ' ');
		try {
			return valueOf(cmds[0]);
		} catch (Exception ex) {
			throw new RuntimeException("Unrecognized response type code: [" + cmd + "]");
		}		
	}
	
	public static ByteBuf extractPayload(final ByteBuf buf) {
		if(buf==null) throw new IllegalArgumentException("The passed buffer was null");
		if(!buf.isReadable()) throw new IllegalArgumentException("The passed buffer was not readable [" + buf + "]");
		final int index = buf.forEachByte(ByteProcessor.FIND_CRLF);
		
		if(index==-1) throw new RuntimeException("Failed to find EOL");
		return buf.slice(index+1, buf.readableBytes() - index-1);
	}


}
