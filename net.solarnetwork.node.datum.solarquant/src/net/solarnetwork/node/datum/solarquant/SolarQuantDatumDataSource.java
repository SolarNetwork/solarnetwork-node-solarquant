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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import net.solarnetwork.node.IdentityService;
import net.solarnetwork.node.MultiDatumDataSource;
import net.solarnetwork.node.domain.GeneralNodePVEnergyDatum;
import net.solarnetwork.node.settings.SettingSpecifier;
import net.solarnetwork.node.settings.SettingSpecifierProvider;
import net.solarnetwork.node.settings.support.BasicTextFieldSettingSpecifier;
import net.solarnetwork.util.OptionalService;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * FIXME
 * 
 * <p>
 * Request the most recent prediction for a source, dump prediction to SolarNet
 * SolarNet overwrites last prediction for node + source
 * </p>
 * 
 * @author matthew
 * @version 1.0
 */
public class SolarQuantDatumDataSource
implements MultiDatumDataSource<GeneralNodePVEnergyDatum>, SettingSpecifierProvider {
	/**
	 * 
	 */
	private final AtomicLong wattHourReading = new AtomicLong(0);
	private final Logger log = LoggerFactory.getLogger(getClass());
	private String sourceId = "Solar";

	private String baseURL = "http://localhost/solarquant/api/prediction/retrieveprediction.php";
	private static HttpURLConnection con;
	OptionalService<IdentityService> identityService;
	private String nodeId = "205";

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

	public SolarQuantDatumDataSource() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getUID() {
		// TODO Auto-generated method stub
		return this.sourceId;
	}

	@Override
	public String getGroupUID() {
		// TODO Auto-generated method stub
		return null;
	}

	// @Override
	public Class<? extends GeneralNodePVEnergyDatum> getDatumType() {
		// TODO Auto-generated method stub
		return GeneralNodePVEnergyDatum.class;
	}

	// @Override
	public GeneralNodePVEnergyDatum readCurrentDatum() {

		int watts = (int) Math.round(Math.random() * 1000.0);

		// we'll increment our Wh reading by a random amount between 0-15, with
		// the assumption we will read samples once per minute
		long wattHours = wattHourReading.addAndGet(Math.round(Math.random() * 15.0));
		GeneralNodePVEnergyDatum datum = new GeneralNodePVEnergyDatum();

		datum.setCreated(new Date());
		datum.setWatts(watts);
		datum.setWattHourReading(wattHours);
		datum.setSourceId(sourceId);
		return datum;

	}

	private MessageSource messageSource;

	@Override
	public String getSettingUID() {
		// TODO Auto-generated method stub
		return "net.solarnetwork.node.datum.solarquant.foobar";
	}

	@Override
	public String getDisplayName() {
		// TODO Auto-generated method stub
		return "SolarQuant";
	}

	@Override
	public MessageSource getMessageSource() {
		// TODO Auto-generated method stub
		return messageSource;
	}

	public void setMessageSource(MessageSource messageSource) {
		this.messageSource = messageSource;
	}

	@Override
	public List<SettingSpecifier> getSettingSpecifiers() {
		SolarQuantDatumDataSource defaults = new SolarQuantDatumDataSource();
		List<SettingSpecifier> results = new ArrayList<SettingSpecifier>(1);
		results.add(new BasicTextFieldSettingSpecifier("sourceId", defaults.sourceId));
		results.add(new BasicTextFieldSettingSpecifier("baseURL", defaults.baseURL));
		results.add(new BasicTextFieldSettingSpecifier("nodeId", defaults.nodeId));
		return results;
	}

	@Override
	public Class<? extends GeneralNodePVEnergyDatum> getMultiDatumType() {
		// TODO Auto-generated method stub
		return GeneralNodePVEnergyDatum.class;
	}

	@Override
	public Collection<GeneralNodePVEnergyDatum> readMultipleDatum() {

		String jsonPrediction = getPredictionJson();
		Collection<GeneralNodePVEnergyDatum> out = null;

		out = decodeJson(jsonPrediction);

		return out;

	}

	private String getPredictionJson() {

		StringBuilder sb = new StringBuilder();
		try {
			URL myurl = new URL(String.format(baseURL + "?nodeId=%s&srcId=%s", getNodeId(), sourceId));
			log.debug(nodeId);
			con = (HttpURLConnection) myurl.openConnection();

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String line;

			while ((line = in.readLine()) != null) {
				sb.append(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	private Collection<GeneralNodePVEnergyDatum> decodeJson(String json) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		Collection<GeneralNodePVEnergyDatum> datumList = new ArrayList<GeneralNodePVEnergyDatum>();

		try {
			ObjectMapper objectMapper = new ObjectMapper();

			JsonNode n = objectMapper.readTree(json);


			for (JsonNode datumNode : n) {
				
				GeneralNodePVEnergyDatum datum = new GeneralNodePVEnergyDatum();


				Date parsedDate = new Date();
				try {
					parsedDate = formatter.parse(datumNode.get("DATE").asText());
				} catch (ParseException e1) {

					e1.printStackTrace();
				}
				
				datum.setCreated(parsedDate);
				datum.setWattHourReading(Long.parseLong(datumNode.get("PREDICTED_WATT_HOURS").asText()));
				datum.setSourceId(sourceId);
				datumList.add(datum);
			}


		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datumList;

	}

	/**
	 * Set the {@link IdentityService} to use.
	 * 
	 * @param identityService
	 *            the service to set
	 */
	public void setIdentityService(OptionalService<IdentityService> identityService) {
		this.identityService = identityService;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public long getNodeId() {
		return Long.parseLong(nodeId);
	}

	private long getNodeIdFromService() {
		IdentityService service = (identityService != null ? identityService.service() : null);
		final Long nodeId = (service != null ? service.getNodeId() : null);
		return (nodeId != null ? nodeId : 0L);
	}

}
