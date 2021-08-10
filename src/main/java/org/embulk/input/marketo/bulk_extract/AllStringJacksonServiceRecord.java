package org.embulk.input.marketo.bulk_extract;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.util.json.JsonParser;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

import java.time.Instant;

public class AllStringJacksonServiceRecord extends JacksonServiceRecord
{
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
            return Double.parseDouble(textValue);
        }

        @Override
        public Value jsonValue(JsonParser jsonParser)
        {
            return jsonParser.parse(textValue);
        }

        @Override
        public long longValue()
        {
            return Long.parseLong(textValue);
        }

        @Override
        public String stringValue()
        {
            return textValue;
        }

        @Override
        public Instant timestampValue(TimestampFormatter timestampFormatter)
        {
            return timestampFormatter.parse(textValue);
        }
    }
}
