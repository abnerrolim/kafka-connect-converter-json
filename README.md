# kafka-connect-converter-json
Default kafka JsonConverter plus hability to provide a default schema.

The motivation to do that is because [JDBC Kafka Connector Sink](https://docs.confluent.io/current/connect/connect-jdbc/docs/sink_connector.html) only accepts records with Schema Registry enabled and others Connectors like [JustOne](https://github.com/justonedb/kafka-sink-pg-json) isn't updated for a long time.
So, if you have a kafka topic with plain json string (without Schema) and this topic is filled always with the same Json structure, you can use JDBC Sink Connector with this Converter. 


##How to Use
First, compile this projec. It's a maven project and you can compile with default maven commands. The kafka-connect-converter-json.jar is a Kafka Connect Plugin and [here](https://docs.confluent.io/current/connect/userguide.html#installing-plugins) is how you install in your kafka connect instance. 


To configure your JDBC Connect, you just need to add  **org.abner.kafka.converter.json.JsonConverterWithDefaultSchema** as your value.converter connector configuration and
write as json our default kafka schema. If you enable **value.converter.schemas.enable** or not define any **value.converter.default.schema.value**, the converter will act like the default JsonConverter implementation.
```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.abner.kafka.converter.json.JsonConverterWithDefaultSchema
key.converter.schemas.enable=false
value.converter.schemas.enable=false
value.converter.default.schema.value={\"name\":\"br.com.zup.realwave.financial.transactions.domain.event.TransactionRealized\",\"version\":1,\"type\":\"struct\",\"fields\":[{\"field\":\"eventid\",\"type\":\"string\"},{\"field\":\"sourcetransactionid\",\"type\":\"string\"},{\"field\":\"sourcetransactiontype\",\"type\":\"string\"},{\"field\":\"transactiontype\",\"type\":\"string\"},{\"field\":\"domain\",\"type\":\"string\"},{\"field\":\"customerid\",\"type\":\"string\"},{\"field\":\"description\",\"type\":\"string\"},{\"field\":\"currency\",\"type\":\"string\"},{\"field\":\"amount\",\"type\":\"int64\"},{\"field\":\"scale\",\"type\":\"int64\"},{\"field\":\"timestamp\",\"type\":\"int64\",\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"version\":1}]}
```

## Defining a Schema
A Schema is just a JSON representation of Schema class, but the Kafka JsonConverter add some extra fields and can be tricky write one by hand and this subject has lack of docs, especially with you need some complex parses.

If you have Decimal, DateTime, Date or Time on your event, you would need to serialize your JSON in a specific format (like unix timestamps to Datetime). You can check these complex data types conversions (named Logical) [on class PreparedStatementBinder](https://github.com/confluentinc/kafka-connect-jdbc/blob/2c47a49e3fcd33bfbefbaae51a534104c038f1c8/src/main/java/io/confluent/connect/jdbc/sink/PreparedStatementBinder.java#L161). In addition to it, you need to name your field with the correct name.
But you can see how these schema's representations are built in the specific classes.
Example: the schema showed as value in **value.converter.default.schema.value** above:
```json
{
   "name":"br.com.zup.realwave.financial.transactions.domain.event.TransactionRealized",
   "version":1,
   "type":"struct",
   "fields":[
      {
         "field":"eventid",
         "type":"string"
      },
      {
         "field":"sourcetransactionid",
         "type":"string"
      },
      {
         "field":"sourcetransactiontype",
         "type":"string"
      },
      {
         "field":"transactiontype",
         "type":"string"
      },
      {
         "field":"domain",
         "type":"string"
      },
      {
         "field":"customerid",
         "type":"string"
      },
      {
         "field":"description",
         "type":"string"
      },
      {
         "field":"currency",
         "type":"string"
      },
      {
         "field":"amount",
         "type":"int64"
      },
      {
         "field":"scale",
         "type":"int64"
      },
      {
         "field":"timestamp",
         "type":"int64",
         "name":"org.apache.kafka.connect.data.Timestamp",
         "version":1
      }
   ]
}
```
The schema above will match with this JSON:
```json
{
   "eventid":"f0114db4-b6ab-4bed-b1eb-7725785b008",
   "sourcetransactionid":"g0214db7-b6ab-4bed-b1eb-7725785b87SA",
   "sourcetransactiontype":"ACCOUNT_CREATED",
   "transactiontype":"CREDIT",
   "domain":"CASH",
   "timestamp":1524740904000,
   "customerid":"1Ae35AS7-e89b-12d3-a456-426655440000",
   "description":"Created",
   "currency":"BRL",
   "amount":1000,
   "scale":2
}
```
As you can see, timestamp need to be serialized as Unix Timestamp and the schema needs to follow the [Timestamp schema](https://github.com/apache/kafka/blob/0.10.2.0/connect/api/src/main/java/org/apache/kafka/connect/data/Timestamp.java)

## Limitations
These limitations aren't specifically of this Converter but of Kafka JDBC Connector and this Converter doesn't try to resolve. 

* Table creation doesn't apply any strategy to resolve CamelCase indentation, so eventId will be created on Postgres as eventid (lowercase) and not event_id, as hibernate does.
* In another hand, table evolution will compare field names strictly, so your message field **eventId** will never match the table field **eventid** and the connector will try to create another column. It's the reason why this example's fields names are all lowercase.
* All your fields should be primitive fields or the logical fields mapped on Kafka (as mentioned above) 
* At this point, you can have only one schema by connector/topic.
