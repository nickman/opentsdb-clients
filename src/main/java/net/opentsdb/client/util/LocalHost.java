/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package net.opentsdb.client.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Pattern;



/**
 * <p>Title: LocalHost</p>
 * <p>Description: An entire class dedicated to trying to figure out what the name or IP address of the current host is</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.utils.net.LocalHost</code></p>
 */

public class LocalHost {
	
    /** The config prop key to specify the exact externally accessible host name */
    public static final String PROP_EXT_HOST_NAME = "host.ext.name";
	/** IP4 address pattern matcher */
	public static final Pattern IP4_ADDRESS = Pattern.compile("((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])");
	/** IP6 address pattern matcher */
	public static final Pattern IP6_ADDRESS = Pattern.compile("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])).*?");
	/** Indicates if we're running on Windows */
	public static final boolean IS_WIN = System.getProperty("os.name", "").toLowerCase().contains("windows");
	/** The JVM's host name according to the RuntimeMXBean */
	public static final String HOST = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];
    /** The system prop key for the RMI host name as provided to remote stubs */
	public static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	
	/** The local wildcard address (<b><code>0.0.0.0</code></b>) */
	public static final InetAddress LOCAL_WILDCARD;
	
	static {
		InetAddress wc = null;
		try {
			wc = InetAddress.getByName("0.0.0.0");
		} catch (Exception ex) {
			wc = null;
		}
		LOCAL_WILDCARD = wc;
	}
	/**
	 * Merges the passed properties
	 * @param properties The properties to merge
	 * @return the merged properties
	 */
	public static Properties mergeProperties(Properties...properties) {
		Properties allProps = new Properties(System.getProperties());
		for(int i = properties.length-1; i>=0; i--) {
			if(properties[i] != null && properties[i].size() >0) {
				allProps.putAll(properties[i]);
			}
		}
		return allProps;
	}
	
	/**
	 * Looks up a property, first in the system properties, then the environment. 
	 * If not found in either, returns the supplied default.
	 * @param name The name of the key to look up.
	 * @param defaultValue The default to return if the name is not found.
	 * @param properties An array of properties to search in. If empty or null, will search system properties. The first located match will be returned.
	 * @return The located value or the default if it was not found.
	 */
	public static String getSystemThenEnvProperty(String name, String defaultValue, Properties...properties) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed property name was null or empty");
		String value = mergeProperties(properties).getProperty(name);
		if(value==null) {
			value = System.getenv(name.replace('.', '_').toUpperCase());
		}
		if(value==null) {
			value=defaultValue;
		}
		return value;
	}	


	/**
	 * Attempts a series of methods of divining the host name
	 * @return the determined host name
	 */
	public static String hostName() {
		String host = getSystemThenEnvProperty(RMI_HOSTNAME, null);
		if(host!=null && !host.isEmpty()) return host;
		host = getSystemThenEnvProperty(PROP_EXT_HOST_NAME, null);
		if(host!=null && !host.isEmpty()) return host;
		host = getHostNameByNic();
		if(host!=null) return host;		
		host = getHostNameByInet();
		if(host!=null) return host;
		host = System.getenv(IS_WIN ? "COMPUTERNAME" : "HOSTNAME");
		if(host!=null && !host.trim().isEmpty()) return host;
		return HOST;
	}	
	
	/**
	 * Uses <b><code>InetAddress.getLocalHost().getCanonicalHostName()</code></b> to get the host name.
	 * If the value is null, empty or equals <b><code>localhost</code></b>, returns null.
	 * @return The host name or null if one was not found.
	 */
	public static String getHostNameByInet() {
		try {
			String inetHost = InetAddress.getLocalHost().getCanonicalHostName();
			if(inetHost==null || inetHost.trim().isEmpty() || "localhost".equalsIgnoreCase(inetHost.trim())) return null;
			return inetHost.trim();
		} catch (Exception x) {
			return null;
		}
	}

	
	/**
	 * Iterates through the found NICs, extracting the host name if the NIC is not the loopback interface.
	 * The host name is extracted from the first address bound to the first matching NIC that does not 
	 * have a canonical name that is an IP address.
	 * @return The host name or null if one was not found.
	 */
	public static String getHostNameByNic() {
		try {
			for(Enumeration<NetworkInterface> nicEnum = NetworkInterface.getNetworkInterfaces(); nicEnum.hasMoreElements();) {
				NetworkInterface nic = nicEnum.nextElement();
				if(nic!=null && nic.isUp() && !nic.isLoopback()) {
					for(Enumeration<InetAddress> nicAddr = nic.getInetAddresses(); nicAddr.hasMoreElements();) {
						InetAddress addr = nicAddr.nextElement();
						String chost = addr.getCanonicalHostName();
						if(chost!=null && !chost.trim().isEmpty()) {
							if(!IP4_ADDRESS.matcher(chost).matches() && !IP6_ADDRESS.matcher(chost).matches()) {
								return chost;
							}
						}
					}
				}
			}
			return null;
		} catch (Exception x) {
			return null;
		}		
	}
	

}
