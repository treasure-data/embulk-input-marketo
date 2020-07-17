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
import org.junit.function.ThrowingRunnable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

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
        activityBulkExtractInputPlugin = Mockito.spy(new ActivityBulkExtractInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/activity_bulk_extract_config.yaml"));
        mockMarketoRestclient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestclient).when(activityBulkExtractInputPlugin).createMarketoRestClient(any(ActivityBulkExtractInputPlugin.PluginTask.class));
    }

    @Test
    public void testInvalidActivityTypeIds()
    {
        configSource.set("activity_type_ids", Arrays.asList(" ", "abc", "123"));
        final ActivityBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(ActivityBulkExtractInputPlugin.PluginTask.class);

        Assert.assertThrows("Invalid activity type id: [ , abc]", ConfigException.class, new ThrowingRunnable()
        {
            @Override
            public void run() throws Throwable
            {
                activityBulkExtractInputPlugin.validateInputTask(task);
            }
        });
    }

    @Test
    public void testActivityTypeIdsValid() throws IOException
    {
        configSource.set("activity_type_ids", Arrays.asList("1", "2", "3 "));
        final ActivityBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(ActivityBulkExtractInputPlugin.PluginTask.class);

        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/activity_types.json"), javaType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());

        Mockito.when(mockMarketoRestclient.getActivityTypes()).thenReturn(mockRecordPagingIterable);
        activityBulkExtractInputPlugin.validateInputTask(task);
    }

    @Test
    public void testRun() throws InterruptedException
    {
        ActivityBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(ActivityBulkExtractInputPlugin.PluginTask.class);

        OffsetDateTime startDate = OffsetDateTime.ofInstant(task.getFromDate().toInstant(), ZoneId.systemDefault());
        List<Integer> activityTypeIds = new ArrayList<>();

        PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        Mockito.when(mockMarketoRestclient.createActivityExtract(any(List.class), any(Date.class), any(Date.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
        Mockito.when(mockMarketoRestclient.getActivitiesBulkExtractResult(Mockito.eq(exportId1), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/activity_extract1.csv"));
        Mockito.when(mockMarketoRestclient.getActivitiesBulkExtractResult(Mockito.eq(exportId2), any(BulkExtractRangeHeader.class))).thenReturn(this.getClass().getResourceAsStream("/fixtures/activity_extract2.csv"));
        ServiceResponseMapper<? extends ValueLocator> mapper = activityBulkExtractInputPlugin.buildServiceResponseMapper(task);
        activityBulkExtractInputPlugin.validateInputTask(task);
        TaskReport taskReport = activityBulkExtractInputPlugin.ingestServiceData(task, mapper.createRecordImporter(), 1, pageBuilder);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Column marketoGUID = mapper.getEmbulkSchema().lookupColumn("marketoGUID");
        Mockito.verify(pageBuilder, Mockito.times(55)).setString(Mockito.eq(marketoGUID), argumentCaptor.capture());
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).startActitvityBulkExtract(Mockito.eq(exportId1));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).waitActitvityExportJobComplete(Mockito.eq(exportId1), Mockito.eq(task.getPollingIntervalSecond()), Mockito.eq(task.getBulkJobTimeoutSecond()));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).startActitvityBulkExtract(Mockito.eq(exportId2));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).waitActitvityExportJobComplete(Mockito.eq(exportId2), Mockito.eq(task.getPollingIntervalSecond()), Mockito.eq(task.getBulkJobTimeoutSecond()));
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createActivityExtract(activityTypeIds, Date.from(startDate.toInstant()), Date.from(startDate.plusDays(30).toInstant()));
        OffsetDateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createActivityExtract(activityTypeIds, Date.from(startDate2.toInstant()), Date.from(startDate.plusDays(task.getFetchDays()).toInstant()));
        ConfigDiff configDiff = activityBulkExtractInputPlugin.buildConfigDiff(task, Mockito.mock(Schema.class), 1, Arrays.asList(taskReport));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Assert.assertEquals(df.format(Date.from(startDate.plusDays(task.getFetchDays()).toInstant())), configDiff.get(String.class, "from_date"));
    }
}
