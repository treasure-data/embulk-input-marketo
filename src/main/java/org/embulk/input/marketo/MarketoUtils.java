package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Sets;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class MarketoUtils
{
    public static final String MARKETO_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z";
    public static final String MARKETO_DATE_FORMAT = "%Y-%m-%d";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
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

    public static final String LIST_ID_COLUMN_NAME = "listId";

    public static final String PROGRAM_ID_COLUMN_NAME = "programId";

    private MarketoUtils()
    {
    }

    public static ServiceResponseMapper<? extends ValueLocator> buildDynamicResponseMapper(String prefix, List<MarketoField> columns)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        for (MarketoField column : columns) {
            String columName = buildColumnName(prefix, column.getName());
            MarketoField.MarketoDataType marketoDataType = column.getMarketoDataType();
            if (marketoDataType.getFormat().isPresent()) {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), columName, marketoDataType.getType(), marketoDataType.getFormat().get());
            }
            else {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), columName, marketoDataType.getType());
            }
        }
        return builder.build();
    }

    public static List<String> getFieldNameFromMarketoFields(List<MarketoField> columns, String... excludedFields)
    {
        Set<String> excludeFields= Sets.newHashSet(excludedFields);
        List<String> extractedFields = new ArrayList<>();
        for (MarketoField column : columns) {
            if (excludeFields.contains(column.getName())) {
                continue;
            }
            extractedFields.add(column.getName());
        }
        return extractedFields;
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

    public static String buildColumnName(String prefix, String columnName)
    {
        return prefix + "_" + columnName;
    }
}
