package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class modify<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Clean special characters from specified fields in a connect record";

  private interface ConfigName {
    String FIELDS_TO_CLEAN = "field.name";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.FIELDS_TO_CLEAN, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
      "Comma-separated list of fields to clean special characters from");

  private static final String PURPOSE = "cleaning special characters from fields";

  private String[] fieldsToClean;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    String fieldsList = config.getString(ConfigName.FIELDS_TO_CLEAN);
    fieldsToClean = fieldsList.split(",");
    // Trim whitespace from field names
    for (int i = 0; i < fieldsToClean.length; i++) {
      fieldsToClean[i] = fieldsToClean[i].trim();
    }

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  @Override
  public R apply(R record) {
    if (operatingValue(record) == null) {
      return record;
    }
    
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
    final Map<String, Object> updatedValue = new HashMap<>(value);

    for (String fieldName : fieldsToClean) {
      if (updatedValue.containsKey(fieldName) && updatedValue.get(fieldName) instanceof String) {
        String fieldValue = (String) updatedValue.get(fieldName);
        if (fieldValue != null) {
          // Replace null characters and other special characters
          String cleanedValue = fieldValue.replaceAll("\\x00", "");
          updatedValue.put(fieldName, cleanedValue);
        }
      }
    }

    return newRecord(record, null, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = value.schema(); // We're not modifying the schema
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }

    final Struct updatedValue = new Struct(updatedSchema);

    // Copy all fields
    for (Field field : value.schema().fields()) {
      Object fieldValue = value.get(field);
      
      // Clean specified string fields
      if (fieldValue instanceof String && contains(fieldsToClean, field.name())) {
        String stringValue = (String) fieldValue;
        // Replace null characters and other special characters
        String cleanedValue = stringValue.replaceAll("\\x00", "");
        updatedValue.put(field.name(), cleanedValue);
      } else {
        updatedValue.put(field.name(), fieldValue);
      }
    }

    return newRecord(record, updatedSchema, updatedValue);
  }

  private boolean contains(String[] array, String value) {
    for (String item : array) {
      if (item.equals(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends modify<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends modify<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


