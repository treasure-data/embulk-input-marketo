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
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
        bulkExtractInputPlugin = Mockito.spy(new LeadBulkExtractInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/lead_bulk_extract_config.yaml"));
        mockMarketoRestclient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestclient).when(bulkExtractInputPlugin).createMarketoRestClient(ArgumentMatchers.any(LeadBulkExtractInputPlugin.PluginTask.class));
    }

    @Test()
    public void testIncrementalWithUpdatedAt()
    {
        configSource.set("incremental", true);
        configSource.set("use_updated_at", true);
        LeadBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(LeadBulkExtractInputPlugin.PluginTask.class);
        bulkExtractInputPlugin.validateInputTask(task);
        MarketoService mockMarketoService = Mockito.mock(MarketoService.class);
        DateTime fromDate = new DateTime(1525792181000L);
        DateTime toDate = fromDate.plusDays(5);
        Mockito.when(mockMarketoService.extractLead(ArgumentMatchers.any(Date.class), ArgumentMatchers.any(Date.class), ArgumentMatchers.any(List.class), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(new File(this.getClass().getResource("/fixtures/lead_extract1.csv").getFile()));
        bulkExtractInputPlugin.getExtractedStream(mockMarketoService, task, fromDate, toDate);
        Mockito.verify(mockMarketoService).extractLead(ArgumentMatchers.eq(fromDate.toDate()), ArgumentMatchers.eq(toDate.toDate()), ArgumentMatchers.any(List.class), ArgumentMatchers.eq(LeadBulkExtractInputPlugin.UPDATED_AT), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt());
    }

    @Test
    public void testRun() throws InterruptedException, IOException
    {
        LeadBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(LeadBulkExtractInputPlugin.PluginTask.class);
        DateTime startDate = new DateTime(task.getFromDate());
        PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        Mockito.when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        Mockito.when(mockMarketoRestclient.createLeadBulkExtract(ArgumentMatchers.any(Date.class), ArgumentMatchers.any(Date.class), ArgumentMatchers.any(List.class), ArgumentMatchers.any(String.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
        Mockito.when(mockMarketoRestclient.getLeadBulkExtractResult(ArgumentMatchers.eq(exportId1), ArgumentMatchers.any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract1.csv"));
        Mockito.when(mockMarketoRestclient.getLeadBulkExtractResult(ArgumentMatchers.eq(exportId2), ArgumentMatchers.any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/leads_extract2.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = bulkExtractInputPlugin.buildServiceResponseMapper(task);
        bulkExtractInputPlugin.validateInputTask(task);
        TaskReport taskReport = bulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
        ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        Column idColumn = mapper.getEmbulkSchema().lookupColumn("mk_id");
        Mockito.verify(pageBuilder, Mockito.times(19)).setLong(ArgumentMatchers.eq(idColumn), argumentCaptor.capture());
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).startLeadBulkExtract(ArgumentMatchers.eq(exportId1));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).waitLeadExportJobComplete(ArgumentMatchers.eq(exportId1), ArgumentMatchers.eq(task.getPollingIntervalSecond()), ArgumentMatchers.eq(task.getBulkJobTimeoutSecond()));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).startLeadBulkExtract(ArgumentMatchers.eq(exportId2));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).waitLeadExportJobComplete(ArgumentMatchers.eq(exportId2), ArgumentMatchers.eq(task.getPollingIntervalSecond()), ArgumentMatchers.eq(task.getBulkJobTimeoutSecond()));
        String filterField = "createdAt";
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createLeadBulkExtract(startDate.toDate(), startDate.plusDays(30).toDate(), fieldNameFromMarketoFields, filterField);
        DateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createLeadBulkExtract(startDate2.toDate(), startDate.plusDays(task.getFetchDays()).toDate(), fieldNameFromMarketoFields, filterField);
        List<Long> leadIds = argumentCaptor.getAllValues();
        Assert.assertEquals(19, leadIds.size());
        ConfigDiff configDiff = bulkExtractInputPlugin.buildConfigDiff(task, Mockito.mock(Schema.class), 1, Arrays.asList(taskReport));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Assert.assertEquals(df.format(startDate.plusDays(task.getFetchDays()).toDate()), configDiff.get(String.class, "from_date"));
        Assert.assertArrayEquals(new Long[]{102488L, 102456L, 102445L, 102439L, 102471L, 102503L, 102424L, 102473L, 102505L, 102492L, 102495L, 102452L, 102435L, 102467L, 102420L, 102496L, 102448L, 102499L, 102431L}, leadIds.toArray());
    }
}
