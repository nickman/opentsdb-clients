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

import java.util.stream.Collectors;

/**
 * <p>Title: StringsUtil</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.client.util.StringsUtil</code></p>
 */

public class StringsUtil {

	  /**
	   * Optimized version of {@code String#split} that doesn't use regexps.
	   * This function works in O(5n) where n is the length of the string to
	   * split.
	   * @param s The string to split.
	   * @param c The separator to use to split the string.
	   * @return A non-null, non-empty array.
	   */
	  public static String[] splitString(final String s, final char c) {
	    final char[] chars = s.toCharArray();
	    int num_substrings = 1;
	    for (final char x : chars) {
	      if (x == c) {
	        num_substrings++;
	      }
	    }
	    final String[] result = new String[num_substrings];
	    final int len = chars.length;
	    int start = 0;  // starting index in chars of the current substring.
	    int pos = 0;    // current index in chars.
	    int i = 0;      // number of the current substring.
	    for (; pos < len; pos++) {
	      if (chars[pos] == c) {
	        result[i++] = new String(chars, start, pos - start);
	        start = pos + 1;
	      }
	    }
	    result[i] = new String(chars, start, pos - start);
	    return result;
	  }
	  
	  /**
	   * Optimized version of {@code String#split} that doesn't use regexps.
	   * This function works in O(5n) where n is the length of the string to
	   * split.
	   * @param s The string to split.
	   * @param c The separator to use to split the string.
	   * @return A non-null, non-empty array.
	   */
	  public static String[] splitCharSequence(final CharSequence s, final char c) {
		 
	    final char[] chars = s.chars().mapToObj(cx -> Character.toString((char) cx)).collect(Collectors.joining()).toCharArray();
	    int num_substrings = 1;
	    for (final char x : chars) {
	      if (x == c) {
	        num_substrings++;
	      }
	    }
	    final String[] result = new String[num_substrings];
	    final int len = chars.length;
	    int start = 0;  // starting index in chars of the current substring.
	    int pos = 0;    // current index in chars.
	    int i = 0;      // number of the current substring.
	    for (; pos < len; pos++) {
	      if (chars[pos] == c) {
	        result[i++] = new String(chars, start, pos - start);
	        start = pos + 1;
	      }
	    }
	    result[i] = new String(chars, start, pos - start);
	    return result;
	  }
	  
	
	private StringsUtil() {}

}
