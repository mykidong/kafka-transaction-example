package mykidong.util;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Map;

public class JsonUtils {

    /**
     * using org jackson.
     *
     * @param mapper
     * @param json
     * @return
     */
    public static Map<String, Object> toMap(ObjectMapper mapper, String json)
    {
        try {
            Map<String, Object> map = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            return map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * using org jackson.
     *
     * @param mapper
     * @param obj
     * @return
     */
    public static String toJson(ObjectMapper mapper, Object obj)
    {
        try {
            return mapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * using com jackson.
     *
     * @param mapper
     * @param json
     * @return
     */
    public static Map<String, Object> toMap(com.fasterxml.jackson.databind.ObjectMapper mapper, String json)
    {
        try {
            Map<String, Object> map = mapper.readValue(json, Map.class);
            return map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * using com jackson.
     *
     * @param mapper
     * @param obj
     * @return
     */
    public static String toJson(com.fasterxml.jackson.databind.ObjectMapper mapper, Object obj)
    {
        try {
            return mapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
