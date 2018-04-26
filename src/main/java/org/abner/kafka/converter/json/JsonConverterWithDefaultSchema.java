package org.abner.kafka.converter.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

public class JsonConverterWithDefaultSchema extends JsonConverter {


    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    private static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final HashMap<Schema.Type, JsonConverterWithDefaultSchema.JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new HashMap();
    private static final HashMap<String, JsonConverterWithDefaultSchema.LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS;
    private static final HashMap<String, JsonConverterWithDefaultSchema.LogicalTypeConverter> TO_JSON_LOGICAL_CONVERTERS;
    private boolean enableSchemas = true;
    private int cacheSize = 1000;
    private Cache<Schema, ObjectNode> fromConnectSchemaCache;
    private Cache<JsonNode, Schema> toConnectSchemaCache;
    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();
    private JsonParser parser = new JsonParser();

    private boolean hasDefaultSchema = false;
    private JsonNode valueSchema;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        Object enableConfigsVal = configs.get("schemas.enable");
        if (enableConfigsVal != null && enableConfigsVal.toString().trim().length() > 2) {
            this.enableSchemas = enableConfigsVal.toString().equals("true");
        }
        Object defaultSchemaString = configs.get("default.schema.value");
        if (defaultSchemaString != null) {
            this.hasDefaultSchema = true;
            valueSchema = parser.serialize(defaultSchemaString.toString());
        }

        this.serializer.configure(configs, isKey);
        this.deserializer.configure(configs, isKey);
        this.fromConnectSchemaCache = new SynchronizedCache(new LRUCache(this.cacheSize));
        this.toConnectSchemaCache = new SynchronizedCache(new LRUCache(this.cacheSize));
    }


    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        Object jsonValue;
        try {
            jsonValue = this.deserializer.deserialize(topic, value);
        } catch (SerializationException var5) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", var5);
        }

        if (this.enableSchemas && (jsonValue == null || !((JsonNode) jsonValue).isObject() || ((JsonNode) jsonValue).size() != 2 || !((JsonNode) jsonValue).has("schema") || !((JsonNode) jsonValue).has("payload"))) {
            throw new DataException("JsonConverter with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields. If you are trying to deserialize plain JSON data, set schemas.enable=false in your converter configuration.");
        }

        if (!this.enableSchemas && hasDefaultSchema) {
            ObjectNode envelope = JsonNodeFactory.instance.objectNode();
            envelope.set("schema", valueSchema);
            envelope.set("payload", (JsonNode) jsonValue);
            jsonValue = envelope;
        } else {
            ObjectNode envelope = JsonNodeFactory.instance.objectNode();
            envelope.set("schema", (JsonNode) null);
            envelope.set("payload", (JsonNode) jsonValue);
            jsonValue = envelope;
        }

        return this.jsonToConnect((JsonNode) jsonValue);
    }

    private SchemaAndValue jsonToConnect(JsonNode jsonValue) {
        if (jsonValue == null) {
            return SchemaAndValue.NULL;
        } else if (jsonValue.isObject() && jsonValue.size() == 2 && jsonValue.has("schema") && jsonValue.has("payload")) {
            Schema schema = this.asConnectSchema(jsonValue.get("schema"));
            return new SchemaAndValue(schema, convertToConnect(schema, jsonValue.get("payload")));
        } else {
            throw new DataException("JSON value converted to Kafka Connect must be in envelope containing schema");
        }
    }

    private Schema asConnectSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull()) {
            return null;
        } else {
            Schema cached = (Schema) this.toConnectSchemaCache.get(jsonSchema);
            if (cached != null) {
                return cached;
            } else {
                JsonNode schemaTypeNode = jsonSchema.get("type");
                if (schemaTypeNode != null && schemaTypeNode.isTextual()) {
                    String var5 = schemaTypeNode.textValue();
                    byte var6 = -1;
                    switch (var5.hashCode()) {
                        case -1325958191:
                            if (var5.equals("double")) {
                                var6 = 6;
                            }
                            break;
                        case -891985903:
                            if (var5.equals("string")) {
                                var6 = 8;
                            }
                            break;
                        case -891974699:
                            if (var5.equals("struct")) {
                                var6 = 11;
                            }
                            break;
                        case 107868:
                            if (var5.equals("map")) {
                                var6 = 10;
                            }
                            break;
                        case 3237417:
                            if (var5.equals("int8")) {
                                var6 = 1;
                            }
                            break;
                        case 64711720:
                            if (var5.equals("boolean")) {
                                var6 = 0;
                            }
                            break;
                        case 93090393:
                            if (var5.equals("array")) {
                                var6 = 9;
                            }
                            break;
                        case 94224491:
                            if (var5.equals("bytes")) {
                                var6 = 7;
                            }
                            break;
                        case 97526364:
                            if (var5.equals("float")) {
                                var6 = 5;
                            }
                            break;
                        case 100359764:
                            if (var5.equals("int16")) {
                                var6 = 2;
                            }
                            break;
                        case 100359822:
                            if (var5.equals("int32")) {
                                var6 = 3;
                            }
                            break;
                        case 100359917:
                            if (var5.equals("int64")) {
                                var6 = 4;
                            }
                    }

                    SchemaBuilder builder;
                    JsonNode schemaVersionNode;
                    JsonNode schemaDocNode;
                    JsonNode schemaParamsNode;
                    JsonNode schemaDefaultNode;
                    JsonNode paramValue;
                    label146:
                    switch (var6) {
                        case 0:
                            builder = SchemaBuilder.bool();
                            break;
                        case 1:
                            builder = SchemaBuilder.int8();
                            break;
                        case 2:
                            builder = SchemaBuilder.int16();
                            break;
                        case 3:
                            builder = SchemaBuilder.int32();
                            break;
                        case 4:
                            builder = SchemaBuilder.int64();
                            break;
                        case 5:
                            builder = SchemaBuilder.float32();
                            break;
                        case 6:
                            builder = SchemaBuilder.float64();
                            break;
                        case 7:
                            builder = SchemaBuilder.bytes();
                            break;
                        case 8:
                            builder = SchemaBuilder.string();
                            break;
                        case 9:
                            schemaVersionNode = jsonSchema.get("items");
                            if (schemaVersionNode == null) {
                                throw new DataException("Array schema did not specify the element type");
                            }

                            builder = SchemaBuilder.array(this.asConnectSchema(schemaVersionNode));
                            break;
                        case 10:
                            schemaDocNode = jsonSchema.get("keys");
                            if (schemaDocNode == null) {
                                throw new DataException("Map schema did not specify the key type");
                            }

                            schemaParamsNode = jsonSchema.get("values");
                            if (schemaParamsNode == null) {
                                throw new DataException("Map schema did not specify the value type");
                            }

                            builder = SchemaBuilder.map(this.asConnectSchema(schemaDocNode), this.asConnectSchema(schemaParamsNode));
                            break;
                        case 11:
                            builder = SchemaBuilder.struct();
                            schemaDefaultNode = jsonSchema.get("fields");
                            if (schemaDefaultNode == null || !schemaDefaultNode.isArray()) {
                                throw new DataException("Struct schema's \"fields\" argument is not an array.");
                            }

                            Iterator i$ = schemaDefaultNode.iterator();

                            while (true) {
                                if (!i$.hasNext()) {
                                    break label146;
                                }

                                paramValue = (JsonNode) i$.next();
                                JsonNode jsonFieldName = paramValue.get("field");
                                if (jsonFieldName == null || !jsonFieldName.isTextual()) {
                                    throw new DataException("Struct schema's field name not specified properly");
                                }

                                builder.field(jsonFieldName.asText(), this.asConnectSchema(paramValue));
                            }
                        default:
                            throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
                    }

                    JsonNode schemaOptionalNode = jsonSchema.get("optional");
                    if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue()) {
                        builder.optional();
                    } else {
                        builder.required();
                    }

                    JsonNode schemaNameNode = jsonSchema.get("name");
                    if (schemaNameNode != null && schemaNameNode.isTextual()) {
                        builder.name(schemaNameNode.textValue());
                    }

                    schemaVersionNode = jsonSchema.get("version");
                    if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
                        builder.version(schemaVersionNode.intValue());
                    }

                    schemaDocNode = jsonSchema.get("doc");
                    if (schemaDocNode != null && schemaDocNode.isTextual()) {
                        builder.doc(schemaDocNode.textValue());
                    }

                    schemaParamsNode = jsonSchema.get("parameters");
                    if (schemaParamsNode != null && schemaParamsNode.isObject()) {
                        Iterator paramsIt = schemaParamsNode.fields();

                        while (paramsIt.hasNext()) {
                            Map.Entry<String, JsonNode> entry = (Map.Entry) paramsIt.next();
                            paramValue = (JsonNode) entry.getValue();
                            if (!paramValue.isTextual()) {
                                throw new DataException("Schema parameters must have string values.");
                            }

                            builder.parameter((String) entry.getKey(), paramValue.textValue());
                        }
                    }

                    schemaDefaultNode = jsonSchema.get("default");
                    if (schemaDefaultNode != null) {
                        builder.defaultValue(convertToConnect(builder, schemaDefaultNode));
                    }

                    Schema result = builder.build();
                    this.toConnectSchemaCache.put(jsonSchema, result);
                    return result;
                } else {
                    throw new DataException("Schema must contain 'type' field");
                }
            }
        }
    }

    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue.isNull()) {
                if (schema.defaultValue() != null) {
                    return schema.defaultValue();
                }

                if (schema.isOptional()) {
                    return null;
                }

                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            switch (jsonValue.getNodeType()) {
                case NULL:
                    return null;
                case BOOLEAN:
                    schemaType = Schema.Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber()) {
                        schemaType = Schema.Type.INT64;
                    } else {
                        schemaType = Schema.Type.FLOAT64;
                    }
                    break;
                case ARRAY:
                    schemaType = Schema.Type.ARRAY;
                    break;
                case OBJECT:
                    schemaType = Schema.Type.MAP;
                    break;
                case STRING:
                    schemaType = Schema.Type.STRING;
                    break;
                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
            }
        }

        JsonConverterWithDefaultSchema.JsonToConnectTypeConverter typeConverter = (JsonConverterWithDefaultSchema.JsonToConnectTypeConverter) TO_CONNECT_CONVERTERS.get(schemaType);

        if (typeConverter == null) {
            throw new DataException("Unknown schema type: " + String.valueOf(schemaType));
        } else {
            Object converted = typeConverter.convert(schema, jsonValue);
            if (schema != null && schema.name() != null) {
                JsonConverterWithDefaultSchema.LogicalTypeConverter logicalConverter = (JsonConverterWithDefaultSchema.LogicalTypeConverter) TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
                if (logicalConverter != null) {
                    converted = logicalConverter.convert(schema, converted);
                }
            }
            return converted;
        }
    }

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.booleanValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return (byte) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return (short) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) { return value.intValue(); }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.longValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.floatValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.doubleValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                try {
                    return value.binaryValue();
                } catch (IOException var4) {
                    throw new DataException("Invalid bytes field", var4);
                }
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                return value.textValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                Schema elemSchema = schema == null ? null : schema.valueSchema();
                ArrayList<Object> result = new ArrayList();
                Iterator i$ = value.iterator();

                while (i$.hasNext()) {
                    JsonNode elem = (JsonNode) i$.next();
                    result.add(JsonConverterWithDefaultSchema.convertToConnect(elemSchema, elem));
                }

                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();
                Map<Object, Object> result = new HashMap();
                Iterator fieldIt;
                if (schema != null && keySchema.type() != Schema.Type.STRING) {
                    if (!value.isArray()) {
                        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    }

                    fieldIt = value.iterator();

                    while (fieldIt.hasNext()) {
                        JsonNode entryx = (JsonNode) fieldIt.next();
                        if (!entryx.isArray()) {
                            throw new DataException("Found invalid map entry instead of array tuple: " + entryx.getNodeType());
                        }

                        if (entryx.size() != 2) {
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entryx.size());
                        }

                        result.put(JsonConverterWithDefaultSchema.convertToConnect(keySchema, entryx.get(0)), JsonConverterWithDefaultSchema.convertToConnect(valueSchema, entryx.get(1)));
                    }
                } else {
                    if (!value.isObject()) {
                        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    }

                    fieldIt = value.fields();

                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = (Map.Entry) fieldIt.next();
                        result.put(entry.getKey(), JsonConverterWithDefaultSchema.convertToConnect(valueSchema, (JsonNode) entry.getValue()));
                    }
                }

                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonConverterWithDefaultSchema.JsonToConnectTypeConverter() {
            public Object convert(Schema schema, JsonNode value) {
                if (!value.isObject()) {
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());
                } else {
                    Struct result = new Struct(schema.schema());
                    Iterator i$ = schema.fields().iterator();

                    while (i$.hasNext()) {
                        Field field = (Field) i$.next();
                        JsonNode jsonNodeValue = value.get(field.name());
                        if (jsonNodeValue == null) {
                            throw new DataException("Unable to find " + field.name() + " on json "+ value);
                        }
                        result.put(field, JsonConverterWithDefaultSchema.convertToConnect(field.schema(), value.get(field.name())));
                    }

                    return result;
                }
            }
        });
        TO_CONNECT_LOGICAL_CONVERTERS = new HashMap();
        TO_CONNECT_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Decimal", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof byte[])) {
                    throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
                } else {
                    return Decimal.toLogical(schema, (byte[]) ((byte[]) value));
                }
            }
        });
        TO_CONNECT_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Date", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer)) {
                    throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
                } else {
                    return Date.toLogical(schema, (Integer) value);
                }
            }
        });
        TO_CONNECT_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Time", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer)) {
                    throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
                } else {
                    return Time.toLogical(schema, (Integer) value);
                }
            }
        });
        TO_CONNECT_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Timestamp", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Long)) {
                    throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
                } else {
                    return Timestamp.toLogical(schema, (Long) value);
                }
            }
        });
        TO_JSON_LOGICAL_CONVERTERS = new HashMap();
        TO_JSON_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Decimal", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof BigDecimal)) {
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                } else {
                    return Decimal.fromLogical(schema, (BigDecimal) value);
                }
            }
        });
        TO_JSON_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Date", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                } else {
                    return Date.fromLogical(schema, (java.util.Date) value);
                }
            }
        });
        TO_JSON_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Time", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                } else {
                    return Time.fromLogical(schema, (java.util.Date) value);
                }
            }
        });
        TO_JSON_LOGICAL_CONVERTERS.put("org.apache.kafka.connect.data.Timestamp", new JsonConverterWithDefaultSchema.LogicalTypeConverter() {
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date)) {
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                } else {
                    return Timestamp.fromLogical(schema, (java.util.Date) value);
                }
            }
        });
    }

    private interface LogicalTypeConverter {
        Object convert(Schema var1, Object var2);
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema var1, JsonNode var2);
    }

}
