package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class LeadBulkExtractInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private LeadBulkExtractInputPlugin bulkExtractInputPlugin;

    private ConfigSource configSource;

    private MarketoRestClient mockMarketoRestclient;

    @Before
    public void prepare() throws IOException
    {
        bulkExtractInputPlugin = spy(new LeadBulkExtractInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/lead_bulk_extract_config.yaml"));
        mockMarketoRestclient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestclient).when(bulkExtractInputPlugin).createMarketoRestClient(any(LeadBulkExtractInputPlugin.PluginTask.class));
    }

    @Test
    public void testRun() throws InterruptedException, IOException
    {
        LeadBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(configSource, LeadBulkExtractInputPlugin.PluginTask.class);
        OffsetDateTime startDate = OffsetDateTime.ofInstant(task.getFromDate().toInstant(), ZoneOffset.UTC);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract1.csv"));
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId2), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/leads_extract2.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        TaskReport taskReport = bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
        ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        Column idColumn = mapper.getEmbulkSchema().lookupColumn("mk_id");
        verify(pageBuilder, times(19)).setLong(eq(idColumn), argumentCaptor.capture());
        verify(mockMarketoRestclient, times(1)).startLeadBulkExtract(eq(exportId1));
        verify(mockMarketoRestclient, times(1)).waitLeadExportJobComplete(eq(exportId1), eq(task.getPollingIntervalSecond()), eq(task.getBulkJobTimeoutSecond()));
        verify(mockMarketoRestclient, times(1)).startLeadBulkExtract(eq(exportId2));
        verify(mockMarketoRestclient, times(1)).waitLeadExportJobComplete(eq(exportId2), eq(task.getPollingIntervalSecond()), eq(task.getBulkJobTimeoutSecond()));
        String filterField = "createdAt";
        verify(mockMarketoRestclient, times(1)).createLeadBulkExtract(Date.from(startDate.toInstant()),
                Date.from(startDate.plusDays(30).toInstant()), fieldNameFromMarketoFields, filterField);
        OffsetDateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        verify(mockMarketoRestclient, times(1)).createLeadBulkExtract(Date.from(startDate2.toInstant()),
                Date.from(startDate.plusDays(task.getFetchDays()).toInstant()), fieldNameFromMarketoFields, filterField);
        List<Long> leadIds = argumentCaptor.getAllValues();
        Assert.assertEquals(19, leadIds.size());
        ConfigDiff configDiff = bulkExtractInputPlugin.buildConfigDiff(task, mock(Schema.class), 1, Arrays.asList(taskReport));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Assert.assertEquals(df.format(Date.from(startDate.plusDays(task.getFetchDays()).toInstant())), configDiff.get(String.class, "from_date"));
        Assert.assertArrayEquals(new Long[]{102488L, 102456L, 102445L, 102439L, 102471L, 102503L, 102424L, 102473L, 102505L, 102492L, 102495L, 102452L, 102435L, 102467L, 102420L, 102496L, 102448L, 102499L, 102431L}, leadIds.toArray());
    }

    @Test
    public void testImportWithoutQuotesInQuotedFieldsConfig() throws IOException
    {
        ConfigSource cfg = configSource.deepCopy().set("included_fields", Arrays.asList("firstName", "lastName"));
        LeadBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(cfg, LeadBulkExtractInputPlugin.PluginTask.class);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";

        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract_with_quote_in_quote_field.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        try {
            bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
            Assert.fail("Expected exception due to quote char in quoted fields");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof CsvTokenizer.InvalidValueException);
            Assert.assertTrue(e.getCause().getMessage().contains("Unexpected extra character 'N' after a value quoted by '\"'"));
        }
    }

    @Test
    public void testImportWithQuotesRFC4180EscapedInvalid() throws IOException
    {
        ConfigSource cfg = configSource.deepCopy()
                .set("fetch_days", 1)
                .set("quotes_in_quoted_fields", "ACCEPT_ONLY_RFC4180_ESCAPED")
                .set("included_fields", Arrays.asList("firstName", "lastName"));
        LeadBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(cfg, LeadBulkExtractInputPlugin.PluginTask.class);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";

        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract_with_quote_in_quote_field.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        try {
            bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
            Assert.fail("Expected exception due to quote chạr in quoted fields");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof CsvTokenizer.InvalidValueException);
            Assert.assertTrue(e.getCause().getMessage().contains("Unexpected extra character 'N' after a value quoted by '\"'"));
        }
    }

    @Test
    public void testImportWithQuotesRFC4180EscapedValid() throws IOException
    {
        ConfigSource cfg = configSource.deepCopy()
                .set("fetch_days", 1)
                .set("quotes_in_quoted_fields", "ACCEPT_ONLY_RFC4180_ESCAPED")
                .set("included_fields", Arrays.asList("firstName", "lastName"));
        LeadBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(cfg, LeadBulkExtractInputPlugin.PluginTask.class);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";

        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract_with_quote_RFC4180.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        try {
            bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
            ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
            Column firstNameColumn = mapper.getEmbulkSchema().lookupColumn("mk_firstName");
            Column lastNameColumn = mapper.getEmbulkSchema().lookupColumn("mk_lastName");
            verify(pageBuilder, times(1)).setString(eq(firstNameColumn), argumentCaptor.capture());
            verify(pageBuilder, times(1)).setString(eq(lastNameColumn), argumentCaptor.capture());
            List<String> values = argumentCaptor.getAllValues();
            Assert.assertEquals(2, values.size());
            Assert.assertEquals("first\"Name1", values.get(0));
            Assert.assertEquals("lastName1", values.get(1));
        }
        catch (Exception e) {
            Assert.fail("Expected no exception due to quote char in quoted fields but got " + e.getMessage());
        }
    }

    @Test
    public void testImportWithStrayQuotes() throws IOException
    {
        ConfigSource cfg = configSource.deepCopy()
                .set("fetch_days", 1)
                .set("quotes_in_quoted_fields", "ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS")
                .set("included_fields", Arrays.asList("firstName", "lastName"));
        LeadBulkExtractInputPlugin.PluginTask task = CONFIG_MAPPER.map(cfg, LeadBulkExtractInputPlugin.PluginTask.class);
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";

        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract_with_quote_in_quote_field.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        try {
            bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
            ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
            Column firstNameColumn = mapper.getEmbulkSchema().lookupColumn("mk_firstName");
            Column lastNameColumn = mapper.getEmbulkSchema().lookupColumn("mk_lastName");
            verify(pageBuilder, times(1)).setString(eq(firstNameColumn), argumentCaptor.capture());
            verify(pageBuilder, times(1)).setString(eq(lastNameColumn), argumentCaptor.capture());
            List<String> values = argumentCaptor.getAllValues();
            Assert.assertEquals(2, values.size());
            Assert.assertEquals("first\"Name1", values.get(0));
            Assert.assertEquals("lastName1", values.get(1));
        }
        catch (Exception e) {
            Assert.fail("Expected no exception due to quote chạr in quoted fields but got " + e.getMessage());
        }
    }
}
