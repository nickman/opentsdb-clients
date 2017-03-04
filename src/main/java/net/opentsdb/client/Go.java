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
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import net.opentsdb.client.json.JSONOps;
import net.opentsdb.client.protocol.UnixClient;
import net.opentsdb.client.tracing.TraceCodec;

/**
 * <p>Title: Go</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.Go</code></p>
 */

/*
 * TODO:
 * ClientConfig:
 * 	set sysprops
 *  set external log config
 *  
 * Client Builder
 * Client Config - fromFile, fromURL, fromString
 * Trace Options:  async/sync, response
 * Dropwizard metrics
 */

public class Go {

	public static final int LOOPS = 5000;
	public static final int SWITCH_METRICS_ON = 1000;
	public static final int TRACES_PER_LOOP = 100;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log("Test Client");
		final ClassLoader cl = Go.class.getClassLoader();
		final MetricRegistry registry = new MetricRegistry();
		// /src/test/resources/configs/unix-json.conf
		try {
			final ThreadLocalRandom r = ThreadLocalRandom.current();
			final File[] clientConfigs = new File("./src/test/resources/configs").listFiles();
//			final File[] clientConfigs = new File[]{new File("./src/test/resources/configs/unix-json.conf")};
//			final File[] clientConfigs = new File[]{new File("./src/test/resources/configs/unix-text.conf")};
//			final File[] clientConfigs = new File[]{new File("./src/test/resources/configs/tcp-json.conf")};
			for(File conf: clientConfigs) {
				
				final ClientConfiguration cc = JSONOps.parseToObject(conf, ClientConfiguration.class);
				final Timer timer = registry.timer(cc.type());
				log(cc.type());
				log("=================================================");
				System.gc();
				final UnixClient client = new UnixClient(cc);
				try {
					for(int i = 0; i < LOOPS; i++) {
						if(i==SWITCH_METRICS_ON) {
							client.enableMetrics(true);
							log("Enabled Metrics");
						}
						final Context ctx = timer.time();
						for(int x = 0; x < TRACES_PER_LOOP; x++) {
							client.trace("super", Math.abs(r.nextInt(999999)), Collections.singletonMap("loop", ""+x));						
						}
						ctx.close();
						//log("Flushing " + client.getCurrentBatchSize() + " datapoints.");
						client.flush();
	//					if(i%3==0) JSONOps.generatorCacheClean();
						
					}
					
					log("Client Stats");
					client.printStats();
					log("Loop Stats:");
					ConsoleReporter.forRegistry(registry).build().report();
					client.close();
					if(cc.encoding()==TraceCodec.JSON) {
						log(JSONOps.generatorCacheStats());
						JSONOps.clearCache();
					}
					registry.removeMatching(MetricFilter.ALL);
					System.gc();
					log("================  Complete [" + cc.type() + "]  =====================\n\n");
				} finally {
					try { client.close(); } catch (Exception x) {/* No Op */}
				}
			}
			
//			System.exit(0);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}

	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}

}
