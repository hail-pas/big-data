package cn.burnish.common.utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonParser {
    public static <T> T getFieldValueFromJson(String json, String fieldName, Class<T> fieldType) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(json);
        JsonNode fieldNode = rootNode.get(fieldName);

        if (fieldNode == null) {
            return null;
        }

        return objectMapper.treeToValue(fieldNode, fieldType);
    }
}

