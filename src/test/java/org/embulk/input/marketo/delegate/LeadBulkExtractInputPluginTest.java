package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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
        LeadBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(LeadBulkExtractInputPlugin.PluginTask.class);
        DateTime startDate = new DateTime(task.getFromDate());
        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), javaType);
        List<String> fieldNameFromMarketoFields = MarketoUtils.getFieldNameFromMarketoFields(marketoFields);
        when(mockMarketoRestclient.describeLead()).thenReturn(marketoFields);
        when(mockMarketoRestclient.createLeadBulkExtract(any(Date.class), any(Date.class), any(List.class), any(String.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId1))).thenReturn(this.getClass().getResourceAsStream("/fixtures/lead_extract1.csv"));
        when(mockMarketoRestclient.getLeadBulkExtractResult(eq(exportId2))).thenReturn(this.getClass().getResourceAsStream("/fixtures/leads_extract2.csv"));
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
        String filterField = "updatedAt";
        verify(mockMarketoRestclient, times(1)).createLeadBulkExtract(startDate.toDate(), startDate.plusDays(30).toDate(), fieldNameFromMarketoFields, filterField);
        DateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        verify(mockMarketoRestclient, times(1)).createLeadBulkExtract(startDate2.toDate(), startDate.plusDays(task.getFetchDays()).toDate(), fieldNameFromMarketoFields, filterField);
        List<Long> leadIds = argumentCaptor.getAllValues();
        assertEquals(19, leadIds.size());
        long latestFetchTime = taskReport.get(Long.class, "latest_fetch_time");
        assertTrue(taskReport.get(Set.class, "latest_uids").isEmpty());
        assertEquals(1504888753000L, latestFetchTime);
        assertArrayEquals(new Long[]{102488L, 102456L, 102445L, 102439L, 102471L, 102503L, 102424L, 102473L, 102505L, 102492L, 102495L, 102452L, 102435L, 102467L, 102420L, 102496L, 102448L, 102499L, 102431L}, leadIds.toArray());
    }
}
