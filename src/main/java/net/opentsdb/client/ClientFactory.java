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

import java.io.File;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.protocol.BaseClient;

/**
 * <p>Title: ClientFactory</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.ClientFactory</code></p>
 */
@SuppressWarnings("unchecked")
public class ClientFactory {
	
	private static final Set<BaseClient<?>> relays = new CopyOnWriteArraySet<BaseClient<?>>();
	
	/** Static class logger */
	protected static final Logger log = LoggerFactory.getLogger(ClientFactory.class);


	
	public static <T extends BaseClient<T>> T client(final ClientConfiguration config) {
		return (T)config.protocol().newClient(config).initialize();
	}

	public static <T extends BaseClient<T>> T client(final File clientConfig) {
		final ClientConfiguration config = JSONOps.parseToObject(clientConfig, ClientConfiguration.class);
		return (T)config.protocol().newClient(config).initialize();
	}
	
	public static <T extends BaseClient<T>> T client(final String clientConfig) {
		final ClientConfiguration config = JSONOps.parseToObject(clientConfig, ClientConfiguration.class);
		return (T)config.protocol().newClient(config).initialize();
	}
	
	public static <T extends BaseClient<T>> T client(final URL clientConfig) {
		final ClientConfiguration config = JSONOps.parseToObject(clientConfig, ClientConfiguration.class);
		return (T)config.protocol().newClient(config).initialize();
	}
	
	public static void relay(final File clientConfig) {
		relay(JSONOps.parseToObject(clientConfig, ClientConfiguration.class));
	}
	
	public static void relay(final URL clientConfig) {
		relay(JSONOps.parseToObject(clientConfig, ClientConfiguration.class));
	}
	
	public static void relay(final String clientConfig) {
		relay(JSONOps.parseToObject(clientConfig, ClientConfiguration.class));
	}
	
	public static void main(String[] args) {
		ClientFactory.relay(new File("./src/test/resources/configs/metrics-relay.conf"));
	}
	
	public static void relay(final ClientConfiguration relayConfig) {
		final BaseClient<?> cli = client(relayConfig);
		final ClientConfiguration cc = cli.getClientConfig();
		final long freq = cc.custom("freq", Number.class, 15000).longValue();
		relays.add(cli);
		final AtomicBoolean keepRunning = new AtomicBoolean(true);
		final String threadName = "MetricsRelayThread[" + cc.type() + ":" + cc.address() + "]";
		final Thread relayThread = new Thread(threadName) {
			public void run() {
				while(keepRunning.get()) {					
					try { Thread.currentThread().join(freq); } catch (Exception x) {/* No Op */}
					cli.requestStats();
				}
				if(keepRunning.compareAndSet(true, false)) {
					log.warn("Relay thread [{}] terminated early");
				}
				
			}
		};
		cc.putCustom("relaythread", relayThread);
		cc.putCustom("relayrunning", keepRunning);
		relayThread.setDaemon(true);
		relayThread.start();
	}
	
	
	static {
		final Thread relayShutdownHook = new Thread("relayShutdownHook") {
			public void run() {
				terminateRelays();
			}
		};
		relayShutdownHook.setDaemon(true);
		Runtime.getRuntime().addShutdownHook(relayShutdownHook);
	}
	
	public static void terminateRelays() {
		for(BaseClient<?> client: relays) {
			final ClientConfiguration cc = client.getClientConfig();
			final String key = cc.type() + ":" + cc.address();
			final AtomicBoolean keepRunning = cc.custom("relayrunning", AtomicBoolean.class, null);
			if(keepRunning!=null) keepRunning.set(false);
			final Thread relayThread = cc.custom("relaythread", Thread.class, null);
			if(relayThread!=null) {
				relayThread.interrupt();
			}
			try { client.close(); } catch (Exception x) {/* No Op */}
			log.info("Stopped relay [{}]", key);
		}
		relays.clear();
	}

	private ClientFactory() {}

}
