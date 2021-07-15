package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.embulk.input.marketo.delegate.ActivityBulkExtractInputPlugin.PluginTask;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class ActivityBulkExtractInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private ActivityBulkExtractInputPlugin activityBulkExtractInputPlugin;

    private ConfigSource configSource;

    private MarketoRestClient mockMarketoRestclient;

    @Before
    public void prepare() throws IOException
    {
        activityBulkExtractInputPlugin = spy(new ActivityBulkExtractInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/activity_bulk_extract_config.yaml"));
        mockMarketoRestclient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestclient).when(activityBulkExtractInputPlugin).createMarketoRestClient(any(PluginTask.class));
    }

    @Test
    public void testInvalidActivityTypeIds()
    {
        configSource.set("activity_type_ids", Arrays.asList(" ", "abc", "123"));
        final PluginTask task = mapTask(configSource);

        Assert.assertThrows("Invalid activity type id: [ , abc]", ConfigException.class, () -> activityBulkExtractInputPlugin.validateInputTask(task));
    }

    @Test
    public void testActivityTypeIdsValid() throws IOException
    {
        configSource.set("activity_type_ids", Arrays.asList("1", "2", "3 "));
        final PluginTask task = mapTask(configSource);

        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/activity_types.json"), javaType);
        when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());

        when(mockMarketoRestclient.getActivityTypes()).thenReturn(mockRecordPagingIterable);
        activityBulkExtractInputPlugin.validateInputTask(task);
    }

    @Test
    public void testRun() throws InterruptedException
    {
        PluginTask task = mapTask(configSource);

        OffsetDateTime startDate = OffsetDateTime.ofInstant(task.getFromDate().toInstant(), ZoneOffset.UTC);
        List<Integer> activityTypeIds = new ArrayList<>();

        PageBuilder pageBuilder = mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        when(mockMarketoRestclient.createActivityExtract(any(List.class), any(Date.class), any(Date.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
        when(mockMarketoRestclient.getActivitiesBulkExtractResult(eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/activity_extract1.csv"));
        when(mockMarketoRestclient.getActivitiesBulkExtractResult(eq(exportId2), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/activity_extract2.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = activityBulkExtractInputPlugin.buildServiceResponseMapper(task);
        activityBulkExtractInputPlugin.validateInputTask(task);
        TaskReport taskReport = activityBulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Column marketoGUID = mapper.getEmbulkSchema().lookupColumn("marketoGUID");
        verify(pageBuilder, times(55)).setString(eq(marketoGUID), argumentCaptor.capture());
        verify(mockMarketoRestclient, times(1)).startActitvityBulkExtract(eq(exportId1));
        verify(mockMarketoRestclient, times(1)).waitActitvityExportJobComplete(eq(exportId1), eq(task.getPollingIntervalSecond()), eq(task.getBulkJobTimeoutSecond()));
        verify(mockMarketoRestclient, times(1)).startActitvityBulkExtract(eq(exportId2));
        verify(mockMarketoRestclient, times(1)).waitActitvityExportJobComplete(eq(exportId2), eq(task.getPollingIntervalSecond()), eq(task.getBulkJobTimeoutSecond()));
        verify(mockMarketoRestclient, times(1)).createActivityExtract(activityTypeIds, Date.from(startDate.toInstant()), Date.from(startDate.plusDays(30).toInstant()));
        OffsetDateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        verify(mockMarketoRestclient, times(1)).createActivityExtract(activityTypeIds, Date.from(startDate2.toInstant()), Date.from(startDate.plusDays(task.getFetchDays()).toInstant()));
        ConfigDiff configDiff = activityBulkExtractInputPlugin.buildConfigDiff(task, mock(Schema.class), 1, Arrays.asList(taskReport));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Assert.assertEquals(df.format(Date.from(startDate.plusDays(task.getFetchDays()).toInstant())), configDiff.get(String.class, "from_date"));
    }

    private PluginTask mapTask(ConfigSource configSource)
    {
        return CONFIG_MAPPER.map(configSource, PluginTask.class);
    }
}
