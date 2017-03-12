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
package net.opentsdb.client.json;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

/**
 * <p>Title: JSONOps</p>
 * <p>Description: Shared JSON utilities</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.json.JSONOps</code></p>
 */

public class JSONOps {

	private static final Cache<OutputStream, JsonGenerator> outputGeneratorCache = CacheBuilder.newBuilder()
			.concurrencyLevel(Runtime.getRuntime().availableProcessors())
			.initialCapacity(56)
			.maximumSize(2048)
			.recordStats()
			.weakKeys()			
			.removalListener(new RemovalListener<OutputStream, JsonGenerator>() {
				@Override
				public void onRemoval(RemovalNotification<OutputStream, JsonGenerator> notification) {
//					System.err.println("Removed Generator");					
				}
			})
			.build();
	
	/** Shared ObjectMapper */
	public static final ObjectMapper objectMapper = new ObjectMapper();
	
	/** Shared JSON factory */
	public static final JsonFactory jfactory = new JsonFactory();
	
	/** Type reference for common string/object maps */
	public static final TypeReference<HashMap<String, Object>> TR_STR_OBJ_HASH_MAP = 
	    new TypeReference<HashMap<String, Object>>() {};
	/** Type reference for common string/string maps */
	public static final TypeReference<HashMap<String, String>> TR_STR_STR_HASH_MAP = 
	    new TypeReference<HashMap<String, String>>() {};
	
	
	
	static {
		final DefaultPrettyPrinter pp = new DefaultPrettyPrinter();		
		objectMapper.setDefaultPrettyPrinter(pp);
	}
	
	public static String generatorCacheStats() {
		return "Size:" + outputGeneratorCache.size() + ", " + outputGeneratorCache.stats();		
	}
	
	public static void clearCache() {
		outputGeneratorCache.invalidateAll();
	}
	
	public static void generatorCacheClean() {
		outputGeneratorCache.cleanUp();
	}
	
	public static void invalidate(final OutputStream os) {
		if(os!=null) outputGeneratorCache.invalidate(os);
	}
	
	/**
	 * Returns a JsonGenerator for the passed output stream.
	 * Calls with the same OutputStream instance should return the same generator
	 * @param os The OutputStream the generator will write to
	 * @param startArray If true, the generator will write a start array when created.
	 * @return the generator
	 */
	public static JsonGenerator generatorFor(final OutputStream os, final boolean startArray) {
		if(os==null) throw new IllegalArgumentException("The passed OutputStream was null");
		try {
			return outputGeneratorCache.get(os, new Callable<JsonGenerator>(){
				@Override
				public JsonGenerator call() throws Exception {
					final JsonGenerator gen =  jfactory.createGenerator(os);
					if(startArray) gen.writeStartArray();
					return gen;
				}
			});
		} catch (Exception ex) {
			throw new RuntimeException("Failed to create JsonGenerator", ex);
		}
	}
	
	/**
	 * Returns a JsonGenerator for the passed output stream.
	 * Calls with the same OutputStream instance should return the same generator
	 * @param os The OutputStream the generator will write to
	 * @return the generator
	 */
	public static JsonGenerator generatorFor(final OutputStream os) {
		return generatorFor(os, false);
	}
	
	/**
	 * Serializes the given object to a JSON string
	 * @param object The object to serialize
	 * @return A JSON formatted string
	 * @throws IllegalArgumentException if the object was null
	 * @throws JSONException if the object could not be serialized
	 */
	public static final String serializeToString(final Object object) {
		if (object == null)
			throw new IllegalArgumentException("Object was null");
		try {
			return objectMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
	}	
	
	/**
	 * Deserializes a JSON formatted string to a specific class type
	 * <b>Note:</b> If you get mapping exceptions you may need to provide a 
	 * TypeReference
	 * @param json The string to deserialize
	 * @param pojo The class type of the object used for deserialization
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or class was null or parsing 
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final String json, final Class<T> pojo) {
		if (json == null || json.isEmpty())
			throw new IllegalArgumentException("Incoming data was null or empty");
		if (pojo == null)
			throw new IllegalArgumentException("Missing class type");

		try {
			return objectMapper.readValue(json, pojo);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Parses the passed stringy into a JsonNode
	 * @param jsonStr The stringy to parse
	 * @return the parsed JsonNode
	 */
	public static JsonNode parseToNode(final CharSequence jsonStr) {
		if (jsonStr == null) throw new IllegalArgumentException("Incoming data was null");
		final String str = jsonStr.toString().trim();
		if (str.isEmpty()) throw new IllegalArgumentException("Incoming data was empty");
		try {
			return objectMapper.readTree(str);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}		
	}
	
	/**
	 * Parses the passed buffer into a JsonNode
	 * @param jsonBytes The buffer to parse
	 * @return the parsed JsonNode
	 */
	public static JsonNode parseToNode(final ByteBuf jsonBytes) {
		if (jsonBytes == null) throw new IllegalArgumentException("Incoming data was null");
		if (!jsonBytes.isReadable()) throw new IllegalArgumentException("Incoming data was empty");
		final ByteBufInputStream bis = new ByteBufInputStream(jsonBytes); 
		try {
			return objectMapper.readTree(bis);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		} finally {
			try { bis.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
		
	
	/**
	 * Deserializes a JSON formatted byte array to a specific class type
	 * @param json The byte array to deserialize
	 * @param type A type definition for a complex object
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or type was null or parsing
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	@SuppressWarnings("unchecked")
	public static final <T> T parseToObject(final byte[] json,
			final TypeReference<T> type) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null");
		if (type == null)
			throw new IllegalArgumentException("Missing type reference");
		try {
			return (T)objectMapper.readValue(json, type);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Deserializes a JSON formatted byte array to a specific class type
	 * @param json The byte array to deserialize
	 * @param type A type definition for a complex object
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or type was null or parsing
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final byte[] json,
			final Class<T> type) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null");
		if (type == null)
			throw new IllegalArgumentException("Missing type reference");
		try {
			return (T)objectMapper.readValue(json, type);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Deserializes a JSON formatted input stream to a specific class type
	 * @param json The input stream to deserialize from
	 * @param type A type definition for a complex object
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or type was null or parsing
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final InputStream json,
			final Class<T> type) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null");
		if (type == null)
			throw new IllegalArgumentException("Missing type reference");
		try {
			return (T)objectMapper.readValue(json, type);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Deserializes a JSON providing URL to a specific class type
	 * @param url The JSON providing URL
	 * @param type A type definition for a complex object
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or type was null or parsing
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final URL url,
			final Class<T> type) {
		if (url == null)
			throw new IllegalArgumentException("URL was null");
		if (type == null)
			throw new IllegalArgumentException("Missing type reference");
		try {
			return (T)objectMapper.readValue(url, type);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}	
	
	/**
	 * Deserializes a JSON node to a specific class type
	 * <b>Note:</b> If you get mapping exceptions you may need to provide a 
	 * TypeReference
	 * @param json The node to deserialize
	 * @param pojo The class type of the object used for deserialization
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or class was null or parsing 
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final JsonNode json, final Class<T> pojo) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null or empty");
		if (pojo == null)
			throw new IllegalArgumentException("Missing class type");
		return objectMapper.convertValue(json, pojo);		
	}
	
	/**
	 * Deserializes a JSON formatted string to a specific class type
	 * <b>Note:</b> If you get mapping exceptions you may need to provide a 
	 * TypeReference
	 * @param json The string to deserialize
	 * @param pojo The class type of the object used for deserialization
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or class was null or parsing 
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final JsonNode json, final TypeReference<T> pojo) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null or empty");
		if (pojo == null)
			throw new IllegalArgumentException("Missing class type");

		try {
			return objectMapper.convertValue(json, pojo);	
		} catch (Exception e) {
			throw new JSONException(e);
		}
	}	
	
	/**
	 * Deserializes a JSON formatted file to a specific class type
	 * @param json The file to deserialize from
	 * @param type A type definition for a complex object
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or type was null or parsing
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final File json,
			final Class<T> type) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null");
		if (type == null)
			throw new IllegalArgumentException("Missing type reference");
		try {
			return (T)objectMapper.readValue(json, type);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}
	
	
	private JSONOps() {}

}
