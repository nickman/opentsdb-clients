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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.unix.DomainSocketAddress;
import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.tracing.TraceCodec;

/**
 * <p>Title: ClientConfiguration</p>
 * <p>Description: Client configuration bean</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.ClientConfiguration</code></p>
 */
@JsonDeserialize(using=ClientConfiguration.ClientConfigurationDeser.class)
@JsonSerialize(using=ClientConfiguration.ClientConfigurationSer.class)
public class ClientConfiguration {
	/** Indicates if we're using ms timestamps (true) or second timestamps (false) */
	protected final boolean msTime;
	/** Indicates if we're using direct allocation byte buffers (true) or heap buffers (false) */
	protected final boolean directBuffers;
	/** Indicates if we're using pooled buffers (true) or unpooled buffers (false) */
	protected final boolean pooledBuffers;
	/** The initial size of the trace buffer in bytes */
	protected final int traceBufferSize;
	/** The data point encoding */
	protected final TraceCodec encoding;
	/** The protocol for the client */
	protected final Protocol protocol;
	/** Indicates if the submitted payload should be gzipped */
	protected final boolean gzip;
	/** The socket address representation of the endpoint */
	protected final SocketAddress address;
	/** Indicates if epoll is disabled even if running on a supported platform */
	protected final boolean disableEpoll;
	
	
	public static void main(String[] args) {
		final ClientConfiguration cc = new ClientConfiguration(true, true, true, 1024,
				TraceCodec.JSON, Protocol.TCP, true, new InetSocketAddress("localhost", 4242), false);
		String jsonText = JSONOps.serializeToString(cc);
		System.out.println(jsonText);
		System.out.println("======================================================================");
		final ClientConfiguration cc2 = JSONOps.parseToObject(jsonText, ClientConfiguration.class);
		System.out.println(cc2);
		InetSocketAddress isa = (InetSocketAddress)cc2.address;
		System.out.println("ISA: host:" + isa.getHostName() + ", port:" + isa.getPort() + ", address:" + isa.getAddress() + ", resolved:" + !isa.isUnresolved());
		System.out.println("======================================================================");
		final ClientConfiguration uc = new ClientConfiguration(true, true, true, 1024,
				TraceCodec.JSON, Protocol.UNIX, true, new DomainSocketAddress("/tmp/opentsdb.sock"), false);
		jsonText = JSONOps.serializeToString(uc);
		System.out.println(jsonText);
		System.out.println("======================================================================");
		final ClientConfiguration uc2 = JSONOps.parseToObject(jsonText, ClientConfiguration.class);
		System.out.println(uc2);
		
	}

	
	public ClientConfiguration(final boolean msTime, final boolean directBuffers, final boolean pooledBuffers, final int traceBufferSize,
			final TraceCodec encoding, final Protocol protocol, final boolean gzip, final SocketAddress address, final boolean disableEpoll) {
		this.msTime = msTime;
		this.directBuffers = directBuffers;
		this.pooledBuffers = pooledBuffers;
		this.traceBufferSize = traceBufferSize;
		this.encoding = encoding;
		this.protocol = protocol;
		this.gzip = gzip;
		this.address = address;
		this.disableEpoll = disableEpoll;
		validate();
	}
	
	private void validate() {
		
	}


	public boolean msTime() {
		return msTime;
	}


	public boolean directBuffers() {
		return directBuffers;
	}


	public boolean pooledBuffers() {
		return pooledBuffers;
	}


	public int traceBufferSize() {
		return traceBufferSize;
	}


	public TraceCodec encoding() {
		return encoding;
	}


	public Protocol protocol() {
		return protocol;
	}


	public boolean gzip() {
		return gzip;
	}
	
	public boolean disableEpoll() {
		return disableEpoll;
	}

	public boolean epoll() {
		return Epoll.isAvailable() && !disableEpoll;
	}

	public SocketAddress address() {
		return address;
	}
	
	public String type() {
		return new StringBuilder(protocol.name()).append("/")
			.append(protocol.channelClass(this).getSimpleName()).append("/")
			.append(encoding).append("/")
			.append(directBuffers ? "DIRECT" : "HEAP").append("/")
			.append(pooledBuffers ? "POOLED" : "UNPOOLED")
			.toString();
	}

	public static class ClientConfigurationSer extends JsonSerializer<ClientConfiguration> {
		@Override
		public void serialize(final ClientConfiguration value, final JsonGenerator gen, final SerializerProvider serializers)
				throws IOException, JsonProcessingException {
			gen.writeStartObject();
			gen.writeBooleanField("mstime", value.msTime);
			gen.writeBooleanField("directbuffers", value.directBuffers);
			gen.writeBooleanField("pooledbuffers", value.pooledBuffers);
			gen.writeNumberField("buffersize", value.traceBufferSize);
			gen.writeStringField("encoding", value.encoding.name());
			gen.writeStringField("protocol", value.protocol.name());
			gen.writeStringField("address", value.address.toString());
			gen.writeBooleanField("disableepoll", value.disableEpoll);
			gen.writeEndObject();			
		}
	}

	public static class ClientConfigurationDeser extends JsonDeserializer<ClientConfiguration> {
		private static final SocketAddress DEFAULT_SOCKET_ADDRESS = new InetSocketAddress("localhost" , 4242); 
		@Override
		public ClientConfiguration deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
			final JsonNode node = p.readValueAsTree();
			boolean msTime = true;
			boolean directBuffers = true;
			boolean pooledBuffers = true;
			int traceBufferSize = 2048;
			TraceCodec encoding = TraceCodec.JSON;
			Protocol protocol = Protocol.TCP;
			boolean gzip = true;
			boolean disableEpoll = false;
			SocketAddress address = DEFAULT_SOCKET_ADDRESS;
			if(node.has("mstime")) msTime = node.get("mstime").booleanValue();
			if(node.has("directbuffers")) directBuffers = node.get("directbuffers").booleanValue();
			if(node.has("pooledbuffers")) pooledBuffers = node.get("pooledbuffers").booleanValue();
			if(node.has("gzip")) gzip = node.get("gzip").booleanValue();
			if(node.has("buffersize")) traceBufferSize = node.get("buffersize").intValue();
			if(node.has("encoding")) encoding = TraceCodec.valueOf(node.get("encoding").textValue().trim().toUpperCase());
			if(node.has("protocol")) protocol = Protocol.valueOf(node.get("protocol").textValue().trim().toUpperCase());
			if(node.has("address")) address = protocol.fromString(node.get("address").textValue());
			if(node.has("disableepoll")) disableEpoll = node.get("disableepoll").booleanValue();
			return new ClientConfiguration(msTime, directBuffers, pooledBuffers, traceBufferSize, encoding, protocol, gzip, address, disableEpoll);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ClientConfiguration [msTime=").append(msTime).append(", directBuffers=").append(directBuffers)
				.append(", pooledBuffers=").append(pooledBuffers).append(", traceBufferSize=").append(traceBufferSize)
				.append(", encoding=").append(encoding).append(", protocol=").append(protocol).append(", gzip=")
				.append(gzip).append(", address=").append(address).append(", disableEpoll=").append(disableEpoll).append("]");
		return builder.toString();
	}



}
