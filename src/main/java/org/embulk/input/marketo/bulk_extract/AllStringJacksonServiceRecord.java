package org.embulk.input.marketo.bulk_extract;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class AllStringJacksonServiceRecord extends JacksonServiceRecord
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AllStringJacksonServiceRecord.class);

    public AllStringJacksonServiceRecord(ObjectNode record)
    {
        super(record);
    }

    @Override
    public JacksonServiceValue getValue(ValueLocator locator)
    {
        // We know that this thing only contain text.
        JacksonServiceValue value = super.getValue(locator);
        return new StringConverterJacksonServiceRecord(value.stringValue());
    }

    private class StringConverterJacksonServiceRecord extends JacksonServiceValue
    {
        private final String textValue;

        public StringConverterJacksonServiceRecord(String textValue)
        {
            super(null);
            this.textValue = textValue;
        }

        @Override
        public boolean isNull()
        {
            return textValue == null || textValue.equals("null");
        }

        @Override
        public boolean booleanValue()
        {
            return Boolean.parseBoolean(textValue);
        }

        @Override
        public double doubleValue()
        {
            try {
                return Double.parseDouble(textValue);
            }
            catch (Exception e) {
                LOGGER.info("skipped to parse Double: " + textValue);
                return Double.NaN;
            }
        }

        @Override
        public Value jsonValue(JsonParser jsonParser)
        {
            try {
                return jsonParser.parse(textValue);
            }
            catch (Exception e) {
                LOGGER.info("skipped to parse JSON: " + textValue);
                return jsonParser.parse("{}");
            }
        }

        @Override
        public long longValue()
        {
            try {
                return Long.parseLong(textValue);
            }
            catch (Exception e) {
                LOGGER.info("skipped to parse Long: " + textValue);
                return Long.MIN_VALUE;
            }
        }

        @Override
        public String stringValue()
        {
            return textValue;
        }

        @Override
        public Instant timestampValue(TimestampFormatter timestampFormatter)
        {
            try {
                return timestampFormatter.parse(textValue);
            }
            catch (Exception e) {
                LOGGER.info("skipped to parse Timestamp: " + textValue);
                return null;
            }
        }
    }
}
