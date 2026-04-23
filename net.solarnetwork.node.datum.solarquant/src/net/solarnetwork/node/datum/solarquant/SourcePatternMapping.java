/* ==================================================================
 * SourcePatternMapping.java - 23/04/2026 4:49:24 pm
 *
 * Copyright 2026 SolarNetwork.net Dev Team
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 * ==================================================================
 */

package net.solarnetwork.node.datum.solarquant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import net.solarnetwork.settings.SettingSpecifier;
import net.solarnetwork.settings.support.BasicTextFieldSettingSpecifier;
import net.solarnetwork.util.StringUtils;

/**
 * A regular expression pattern search and replace style mapping for a source
 * ID.
 *
 * @author matt
 * @version 1.0
 */
public class SourcePatternMapping {

	private Pattern search;
	private String replace;

	/**
	 * Constructor.
	 */
	public SourcePatternMapping() {
		super();
	}

	/**
	 * Constructor.
	 *
	 * @param search
	 *        the search regular expression
	 * @param replace
	 *        the replace pattern
	 */
	public SourcePatternMapping(String search, String replace) {
		this(Pattern.compile(search, Pattern.CASE_INSENSITIVE), replace);
	}

	/**
	 * Constructor.
	 *
	 * @param search
	 *        the search regular expression
	 * @param replace
	 *        the replace pattern
	 */
	public SourcePatternMapping(Pattern search, String replace) {
		super();
		this.search = search;
		this.replace = replace;
	}

	/**
	 * Get settings for configuring an instance of this class.
	 *
	 * @param prefix
	 *        the optional prefix to use
	 * @return the settings
	 */
	public static List<SettingSpecifier> settings(String prefix) {
		prefix = (prefix != null ? prefix : "");
		List<SettingSpecifier> result = new ArrayList<>(2);
		result.add(new BasicTextFieldSettingSpecifier(prefix + "searchValue", null));
		result.add(new BasicTextFieldSettingSpecifier(prefix + "replace", null));
		return result;
	}

	/**
	 * Resolve a source ID from a set of mappings.
	 *
	 * <p>
	 * Search pattern capture groups can be referenced as {@code {n}} in replace
	 * values, where {@code n} is the capture group index, starting from
	 * {@code 1}.
	 * </p>
	 *
	 * @param sourceId
	 *        the source ID to resolve
	 * @param mappings
	 *        the set of mappings to apply
	 * @return if any argument is {@code null}, then {@code sourceId} will be
	 *         returned; otherwise, the first search pattern in {@code mappings}
	 *         that matches will be used to resolve the output source ID using
	 *         its {@code replace} value; if no search pattern matches,
	 *         {@code sourceId} will be returned
	 */
	public static String resolveSourceId(String sourceId, SourcePatternMapping[] mappings) {
		if ( sourceId == null || mappings == null || mappings.length < 1 ) {
			return sourceId;
		}
		for ( SourcePatternMapping mapping : mappings ) {
			Pattern s = mapping.search;
			String r = mapping.replace;
			if ( s == null || r == null || r.isEmpty() ) {
				continue;
			}
			String[] match = StringUtils.match(s, sourceId);
			if ( match == null ) {
				continue;
			}
			Map<String, Object> params = new HashMap<>(match.length);
			params.put("s", sourceId);
			for ( int i = 1; i < match.length; i++ ) {
				params.put(String.valueOf(i), match[i]);
			}
			return StringUtils.expandTemplateString(r, params);
		}
		return sourceId;
	}

	/**
	 * Get the search pattern.
	 *
	 * @return the search pattern
	 */
	public final Pattern getSearch() {
		return search;
	}

	/**
	 * Set the search pattern.
	 *
	 * @param search
	 *        the search to set
	 */
	public final void setSearch(Pattern search) {
		this.search = search;
	}

	/**
	 * Get the search pattern as a regular expression string.
	 *
	 * @return the search pattern
	 */
	public final String getSearchValue() {
		return (search != null ? search.pattern() : null);
	}

	/**
	 * Set the search pattern as a regular expression string.
	 *
	 * @param search
	 *        the search to set
	 */
	public final void setSearchValue(String search) {
		this.search = (search != null && !search.isEmpty()
				? Pattern.compile(search, Pattern.CASE_INSENSITIVE)
				: null);
	}

	/**
	 * Get the replace template.
	 *
	 * @return the replace
	 */
	public final String getReplace() {
		return replace;
	}

	/**
	 * Set the replace template.
	 *
	 * @param replace
	 *        the replace to set
	 */
	public final void setReplace(String replace) {
		this.replace = replace;
	}

}
