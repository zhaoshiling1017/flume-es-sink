package org.apache.flume.sink.elasticsearch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchJsonSerializer implements ElasticSearchEventSerializer {
  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchJsonSerializer.class);
  
  public void configure(ComponentConfiguration conf) {}
  
  public void configure(Context context) {}
  
  public List<String> getContentBuilder(Event event) throws IOException {
    byte[] body = event.getBody();
    try {
      return formatJson(body);
    } catch (Exception e) {
    }
    return null;
  }
  
  private List<String> formatJson(byte[] body) throws JSONException {
    List<String> records = new LinkedList<String>();
    String jsonStr = new String(body);
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("get input json in ElasticSearchJsonSerializer: %s", new Object[] { jsonStr }));
    }
    String[] jsonList = jsonStr.split("\\r?\\n");
    for (String json : jsonList) {
      if (!json.isEmpty()) {
    	for (Object mapKey : ElasticSearchFieldMapManager.getInstance().getMapPropeties().keySet()) {
    	  String key = (String)mapKey;
          json = json.replace(key, ElasticSearchFieldMapManager.getInstance().map2NewFieldName(key));
        }
        boolean isJson = false;
        try {
          JSONObject jsonObject = new JSONObject(json);
          isJson = true;
          records.add(jsonObject.toString());
        } catch (JSONException ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("jsonStr is not JSONObject");
          }
        }
        try {
          JSONArray jsonArray = new JSONArray(json);
          isJson = true;
          for (int i = 0; i < jsonArray.length(); i++) {
            records.add(jsonArray.getJSONObject(i).toString());
          }
        } catch (JSONException ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("jsonStr is not JSONArray");
          }
        }
        if (!isJson) {
          logger.warn(String.format("input string is not json format: %s", new Object[] { json }));
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("get final record string for es: %s", new Object[] { records }));
    }
    return records;
  }
}