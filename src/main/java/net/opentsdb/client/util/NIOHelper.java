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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * <p>Title: NIOHelper</p>
 * <p>Description: Some NIO related utility methods</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.util.NIOHelper</code></p>
 */

public class NIOHelper {
	/** The direct byte buff class */
	private static final Class<?> directByteBuffClass;
	/** The direct byte buff class cleaner accessor method*/
	private static final Method getCleanerMethod;
	/** The clean method in cleaner */
	private static final Method cleanMethod;
	
	private static final boolean IS_WIN = System.getProperty("os.name").toLowerCase().contains("windows");
	
	static {
		Class<?> clazz = null;
		Class<?> cleanerClazz = null;
		Method m = null;
		Method cm = null;
		try {
			clazz = Class.forName("java.nio.DirectByteBuffer", true, ClassLoader.getSystemClassLoader());
			m = clazz.getDeclaredMethod("cleaner");
			m.setAccessible(true);
			cleanerClazz = Class.forName("sun.misc.Cleaner", true, ClassLoader.getSystemClassLoader());
			cm = cleanerClazz.getDeclaredMethod("clean");
			cm.setAccessible(true);
		} catch (Throwable x) {
			clazz = null;
			m = null;
			cm = null;
			System.err.println("Failed to initialize DirectByteBuffer Cleaner:" + x + "\n\tNon-Fatal, will continue");
		}
		directByteBuffClass = clazz;
		getCleanerMethod = m;
		cleanMethod = cm;
	}
	

	
	
	/**
	 * Manual deallocation of the memory allocated for direct byte buffers.
	 * Does nothing if the cleaner class and methods were not reflected successfully,
	 * the passed buffer is null or not a DirectByteBuffer, 
	 * or if the clean invocation fails.
	 * @param buffs The buffers to clean
	 */
	public static void clean(final Buffer... buffs) {
		if(buffs!=null) {
			for(Buffer buff: buffs) {
				if(buff==null) continue;
				if(directByteBuffClass!=null && directByteBuffClass.isInstance(buff)) {
					try {
						Object cleaner = getCleanerMethod.invoke(buff);
						if(cleaner!=null) {
							cleanMethod.invoke(cleaner);					
						}
						return;
					} catch (Throwable t) {
						t.printStackTrace(System.err);
						/* No Op */
					}
				}
				if(IS_WIN) System.err.println("Uncleaned MappedByteBuffer on Windows !!!!");				
			}
		}
	}

	/**
	 * Loads the passed file into a direct byte buffer and returns it.
	 * If the platform is windows, the buffer will be disconnected.
	 * @param file The file to load
	 * @return the byte buffer containng the file content
	 */
	public static ByteBuffer load(final File file) {
		return load(file, IS_WIN);
	}

	/**
	 * Loads the passed file into a direct byte buffer and returns it.
	 * @param file The file to load
	 * @param disconnect if true, the mapped file is written into a disconnected byte buffer and the mapped file buffer is closed and cleaned.
	 * @return the byte buffer containing the file content
	 */
	public static ByteBuffer load(final File file, final boolean disconnect) {
		if(file==null) throw new IllegalArgumentException("The passed file is null");
		if(!file.canRead()) throw new IllegalArgumentException("The passed file [" + file + "] cannot be read");
		RandomAccessFile raf = null;
		FileChannel fc = null;
		try {
			final long size = file.length();
			raf = new RandomAccessFile(file, "r");
			fc = raf.getChannel();
			MappedByteBuffer mbb = fc.map(MapMode.READ_ONLY, 0, size);
			if(disconnect && size < Integer.MAX_VALUE) {
				mbb.load();
				ByteBuffer bb = ByteBuffer.allocateDirect((int)size);
				bb.put(mbb);
				bb.flip();
				clean(mbb);
				return bb;
			}
			return mbb;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to load file [" + file + "]", ex);
		} finally {
			if(fc!=null) try { fc.close(); } catch (Exception x) {/* No Op */}
			if(raf!=null) try { raf.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Loads the content from the passed URL into a direct byte buffer and returns it.
	 * @param url The url to load from
	 * @return the byte buffer containing the url content
	 */
	public static ByteBuffer load(final URL url) {
		if(url==null) throw new IllegalArgumentException("The passed url is null");
		InputStream is = null;
		BufferedInputStream bis = null;
		File tmpFile = null;
		FileOutputStream fos = null;
		BufferedOutputStream bos = null;
		try {
			is = url.openStream();
			if(is==null) throw new IllegalArgumentException("The passed url [" + url + "] could not start an input stream");
			bis = new BufferedInputStream(is, 8096);
			tmpFile = File.createTempFile("loadurlfile", ".tmp");
			fos = new FileOutputStream(tmpFile);
			bos = new BufferedOutputStream(bos);
			final byte[] buff = new byte[8096];
			int bytesRead = -1;
			while((bytesRead = bis.read(buff))!=-1) {
				bos.write(buff, 0, bytesRead);
			}
			try { bos.flush(); } catch (Exception x) {/* No Op */}
			try { bos.close(); } catch (Exception x) {/* No Op */}
			try { fos.flush(); } catch (Exception x) {/* No Op */}
			try { fos.close(); } catch (Exception x) {/* No Op */}
			return load(tmpFile, IS_WIN);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to load url [" + url + "]", ex);
		} finally {
			if(bos!=null) {
				try { bos.flush(); } catch (Exception x) {/* No Op */}
				try { bos.close(); } catch (Exception x) {/* No Op */}
			}			
			if(fos!=null) {
				try { fos.flush(); } catch (Exception x) {/* No Op */}
				try { fos.close(); } catch (Exception x) {/* No Op */}
			}
			if(bis!=null) try { bis.close(); } catch (Exception x) {/* No Op */} 
			if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
			if(tmpFile!=null) {
				if(!tmpFile.delete()) tmpFile.deleteOnExit();
			}
		}		
	}
	
	
	private NIOHelper() {}


}
