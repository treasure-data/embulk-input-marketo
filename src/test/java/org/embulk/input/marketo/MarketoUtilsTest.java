package org.embulk.input.marketo;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Column;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by tai.khuu on 10/7/17.
 */
public class MarketoUtilsTest
{
    @Test
    public void buildDynamicResponseMapper() throws Exception
    {
        List<MarketoField> marketoFields = new ArrayList<>();
        marketoFields.add(new MarketoField("marketoField1", "text"));
        marketoFields.add(new MarketoField("marketoField2", "date"));
        marketoFields.add(new MarketoField("marketoField3", "datetime"));
        ServiceResponseMapper<? extends ValueLocator> mapper = MarketoUtils.buildDynamicResponseMapper("marketo", marketoFields);
        List<Column> columns = mapper.getEmbulkSchema().getColumns();
        Column column1 = columns.get(0);
        assertEquals("marketo_marketoField1", column1.getName());
        assertEquals(Types.STRING, column1.getType());
        Column column2 = columns.get(1);
        assertEquals(Types.TIMESTAMP, column2.getType());
        assertEquals("marketo_marketoField2", column2.getName());
        Column column3 = columns.get(2);
        assertEquals("marketo_marketoField3", column3.getName());
        assertEquals(Types.TIMESTAMP, column3.getType());
    }

    @Test
    public void getFieldNameFromMarketoFields() throws Exception
    {
        List<MarketoField> marketoFields = new ArrayList<>();
        marketoFields.add(new MarketoField("marketoField1", "text"));
        marketoFields.add(new MarketoField("marketoField2", "date"));
        marketoFields.add(new MarketoField("marketoField3", "datetime"));
        List<String> marketoFieldList = MarketoUtils.getFieldNameFromMarketoFields(marketoFields, "marketoField2");
        assertEquals(2, marketoFieldList.size());
        assertFalse(marketoFieldList.contains("marketoField2"));
    }

    @Test
    public void buildColumnName() throws Exception
    {
        String columnName = MarketoUtils.buildColumnName("prefix", "columnName");
        assertEquals("prefix_columnName", columnName);
        String noPrefixColumn = MarketoUtils.buildColumnName("", "columnName");
        assertEquals("columnName", noPrefixColumn);
    }

    @Test
    public void getIdentityEndPoint() throws Exception
    {
        String identityEndPoint = MarketoUtils.getIdentityEndPoint("accountId");
        assertEquals("https://accountId.mktorest.com/identity", identityEndPoint);
    }

    @Test
    public void getEndPoint() throws Exception
    {
        String endPoint = MarketoUtils.getEndPoint("accountId");
        assertEquals("https://accountId.mktorest.com", endPoint);
    }

    @Test
    public void sliceRange() throws Exception
    {
        DateTime startDate = new DateTime(1507369760000L);
        List<MarketoUtils.DateRange> dateRanges1 = MarketoUtils.sliceRange(startDate, startDate.plusDays(7), 2);
        assertEquals(4, dateRanges1.size());
        assertEquals(startDate.plusDays(7), dateRanges1.get(3).toDate);

        List<MarketoUtils.DateRange> dateRanges2 = MarketoUtils.sliceRange(startDate, startDate.plusDays(1), 2);
        assertEquals(1, dateRanges2.size());
        assertEquals(startDate.plusDays(1), dateRanges2.get(0).toDate);
    }
}
