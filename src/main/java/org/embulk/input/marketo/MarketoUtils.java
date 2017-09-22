package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class MarketoUtils
{
    private static final Logger LOGGER = Exec.getLogger(MarketoUtils.class);
    public static final String MARKETO_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z";
    public static final String MARKETO_DATE_FORMAT = "%Y-%m-%d";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final Function<ObjectNode, ServiceRecord> TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION = new Function<ObjectNode, ServiceRecord>()
    {
        @Nullable
        @Override
        public JacksonServiceRecord apply(@Nullable ObjectNode input)
        {
            return new JacksonServiceRecord(input);
        }
    };

    public static final String MARKETO_DATE_SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    private MarketoUtils()
    {
    }

    public static ServiceResponseMapper<? extends ValueLocator> buildDynamicResponseMapper(List<MarketoField> columns)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        for (MarketoField column : columns) {
            MarketoField.MarketoDataType marketoDataType = column.getMarketoDataType();
            if (marketoDataType.getFormat().isPresent()) {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), column.getName(), marketoDataType.getType(), marketoDataType.getFormat().get());
            }
            else {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), column.getName(), marketoDataType.getType());
            }
        }
        return builder.build();
    }

    public static List<String> getFieldNameFromSchema(Schema schema)
    {
        return FluentIterable.from(schema.getColumns()).transform(new Function<Column, String>()
        {
            @Override
            public String apply(Column input)
            {
                return input.getName();
            }
        }).toList();
    }
    public static  ObjectNode transformToObjectNode(final Map<String, String> kvMap, Schema schema)
    {
        final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        schema.visitColumns(new ColumnVisitor()
        {
            @Override
            public void booleanColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                objectNode.put(column.getName(), s == null ? null : Boolean.parseBoolean(s));
            }

            @Override
            public void longColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                long l = 0;
                try {
                    l = Long.parseLong(s);
                }
                catch (NumberFormatException ex) {
                    if (!Strings.isNullOrEmpty(s)) {
                        LOGGER.error("Can't convert value : [{}] to long", s, ex);
                        throw new DataException("Can't parse value to long");
                    }
                }
                objectNode.put(column.getName(), l);
            }

            @Override
            public void doubleColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                try {
                    objectNode.put(column.getName(), s == null ? null : Double.parseDouble(s));
                }
                catch (NumberFormatException ex) {
                    if (!Strings.isNullOrEmpty(s)) {
                        LOGGER.error("Can't convert value : [{}] to double", s, ex);
                        throw new DataException("Can't parse value to double");
                    }
                }
            }

            @Override
            public void stringColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                objectNode.put(column.getName(), s);
            }

            @Override
            public void timestampColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                objectNode.put(column.getName(), s);
            }

            @Override
            public void jsonColumn(Column column)
            {
                String s = kvMap.get(column.getName());
                try {
                    objectNode.set(column.getName(), s != null ? OBJECT_MAPPER.readTree(s) : null);
                }
                catch (IOException e) {
                    LOGGER.error("Can't convert column [{}] value [{}] to json", column.getName(), s, e);
                    throw new DataException("Can't convert column " + column.getName() + " value to json");
                }
            }
        });
        return objectNode;
    }

    public static  <K, V> Map<K, V> zip(List<K> keys, List<V> values)
    {
        Map<K, V> kvMap = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            kvMap.put(keys.get(i), values.get(i));
        }
        return kvMap;
    }

    public static Date addDate(Date fromDate, Integer addDate)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(fromDate);
        c.add(Calendar.DATE, addDate);
        return c.getTime();
    }
}
