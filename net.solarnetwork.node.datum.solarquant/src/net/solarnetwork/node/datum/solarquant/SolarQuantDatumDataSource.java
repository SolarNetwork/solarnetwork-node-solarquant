/* ==================================================================
 * TestDatumDataSource.java - 13/02/2018 2:21:47 PM
 * 
 * Copyright 2018 SolarNetwork.net Dev Team
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.springframework.util.FileCopyUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.solarnetwork.codec.JsonUtils;
import net.solarnetwork.domain.datum.DatumSamples;
import net.solarnetwork.node.domain.datum.NodeDatum;
import net.solarnetwork.node.domain.datum.SimpleEnergyDatum;
import net.solarnetwork.node.service.MultiDatumDataSource;
import net.solarnetwork.node.service.support.DatumDataSourceSupport;
import net.solarnetwork.settings.SettingSpecifier;
import net.solarnetwork.settings.SettingSpecifierProvider;
import net.solarnetwork.settings.support.BasicTextFieldSettingSpecifier;
import net.solarnetwork.util.DateUtils;

/**
 * Collect prediction energy values as a datum stream from a SolarQuant server.
 * 
 * <p>
 * Request the most recent prediction for a source, dump prediction to SolarNet
 * SolarNet overwrites last prediction for node + source
 * </p>
 * 
 * @author matthew
 * @version 2.0
 */
public class SolarQuantDatumDataSource extends DatumDataSourceSupport
		implements MultiDatumDataSource, SettingSpecifierProvider {

	public static final String DEFAULT_BASE_URL = "http://localhost/solarquant/api/prediction/retrieveprediction.php";

	private String sourceId;
	private String baseURL = DEFAULT_BASE_URL;
	private Long nodeId;

	@Override
	public String getSettingUid() {
		return "net.solarnetwork.node.datum.solarquant";
	}

	@Override
	public String getDisplayName() {
		return "SolarQuant Datum Source";
	}

	@Override
	public List<SettingSpecifier> getSettingSpecifiers() {
		List<SettingSpecifier> result = getIdentifiableSettingSpecifiers();
		result.add(new BasicTextFieldSettingSpecifier("baseURL", DEFAULT_BASE_URL));
		result.add(new BasicTextFieldSettingSpecifier("sourceId", null));
		result.add(new BasicTextFieldSettingSpecifier("nodeId", null));
		return result;
	}

	@Override
	public Class<? extends NodeDatum> getMultiDatumType() {
		return net.solarnetwork.node.domain.datum.EnergyDatum.class;
	}

	@Override
	public Collection<NodeDatum> readMultipleDatum() {
		final Long nodeId = getNodeId();
		final String sourceId = getSourceId();
		if ( nodeId == null || sourceId == null || sourceId.trim().isEmpty() ) {
			return Collections.emptyList();
		}
		final String jsonPrediction = getPredictionJson(nodeId, sourceId);
		log.trace("Got SolarQuant JSON response: {}", jsonPrediction);
		return decodeJson(jsonPrediction, sourceId);
	}

	private String getPredictionJson(final Long nodeId, final String sourceId) {
		StringBuilder sb = new StringBuilder();
		try {
			URL url = new URL(String.format(baseURL + "?nodeId=%s&srcId=%s", nodeId, sourceId));
			log.debug("Requesting SolarQuant predictions from [{}]", url);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			return FileCopyUtils
					.copyToString(new BufferedReader(new InputStreamReader(con.getInputStream())));
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	private Collection<NodeDatum> decodeJson(final String json, final String sourceId) {
		Collection<NodeDatum> datumList = new ArrayList<>();

		try {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode n = objectMapper.readTree(json);

			for ( JsonNode datumNode : n ) {
				Instant t = JsonUtils.parseDateAttribute(datumNode, "DATE",
						DateUtils.ISO_DATE_TIME_ALT_UTC, Instant::from);

				SimpleEnergyDatum datum = new SimpleEnergyDatum(sourceId, t, new DatumSamples());
				datum.setWattHourReading(Long.parseLong(datumNode.get("PREDICTED_WATT_HOURS").asText()));
				datumList.add(datum);
			}
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		return datumList;

	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public String getSourceId() {
		return this.sourceId;
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	public String getBaseURL() {
		return this.baseURL;
	}

	public void setNodeId(Long nodeId) {
		this.nodeId = nodeId;
	}

	public Long getNodeId() {
		return nodeId;
	}

}
