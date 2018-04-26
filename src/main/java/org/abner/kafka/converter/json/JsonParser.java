package org.abner.kafka.converter.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;

public class JsonParser {
    private ObjectMapper objectMapper = new ObjectMapper();


    public JsonNode serialize(String payload) {
            try {
                JsonNode data = this.objectMapper.readTree(payload);
                return data;
            } catch (Exception var5) {
                throw new SerializationException(var5);
            }
    }
}
