package org.embulk.input.marketo.model;

import org.embulk.input.marketo.model.filter.MarketoFilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tai.khuu on 8/27/17.
 */
public class MarketoBulkExtractRequest
{
    private List<String> fields;
    private String format;

    private Map<String, String> columnHeaderNames;

    private Map<String, MarketoFilter> filter = new HashMap<>();

    public List<String> getFields()
    {
        return fields;
    }

    public void setFields(List<String> fields)
    {
        this.fields = fields;
    }

    public String getFormat()
    {
        return format;
    }

    public void setFormat(String format)
    {
        this.format = format;
    }

    public Map<String, String> getColumnHeaderNames()
    {
        return columnHeaderNames;
    }

    public void setColumnHeaderNames(Map<String, String> columnHeaderNames)
    {
        this.columnHeaderNames = columnHeaderNames;
    }

    public Map<String, MarketoFilter> getFilter()
    {
        return filter;
    }

    public void setFilter(Map<String, MarketoFilter> filter)
    {
        this.filter = filter;
    }

    @Override
    public String toString()
    {
        return "MarketoBulkExtractRequest{" +
                "format='" + format + '\'' +
                ", columnHeaderNames=" + columnHeaderNames +
                ", filter=" + filter +
                '}';
    }
}
