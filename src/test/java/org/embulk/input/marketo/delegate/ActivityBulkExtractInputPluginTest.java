package org.embulk.input.marketo.delegate;

import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class ActivityBulkExtractInputPluginTest
{
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
    public void testRun() throws InterruptedException
    {
        ActivityBulkExtractInputPlugin.PluginTask task = configSource.loadConfig(ActivityBulkExtractInputPlugin.PluginTask.class);
        DateTime startDate = new DateTime(task.getFromDate());
        PageBuilder pageBuilder = Mockito.mock(PageBuilder.class);
        String exportId1 = "exportId1";
        String exportId2 = "exportId2";
        Mockito.when(mockMarketoRestclient.createActivityExtract(any(Date.class), any(Date.class))).thenReturn(exportId1).thenReturn(exportId2).thenReturn(null);
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
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createActivityExtract(startDate.toDate(), startDate.plusDays(30).toDate());
        DateTime startDate2 = startDate.plusDays(30).plusSeconds(1);
        Mockito.verify(mockMarketoRestclient, Mockito.times(1)).createActivityExtract(startDate2.toDate(), startDate.plusDays(task.getFetchDays()).toDate());
        List<String> marketoUids = argumentCaptor.getAllValues();
        assertEquals(55, marketoUids.size());
        long latestFetchTime = taskReport.get(Long.class, "latest_fetch_time");
        Set latestUids = taskReport.get(Set.class, "latest_uids");
        assertEquals(1504888754000L, latestFetchTime);
        assertEquals(Arrays.asList("558681", "558682", "558683", "558684", "558685", "558686", "558687", "558688", "558689", "558690", "558691", "558692", "558693", "558694", "558695", "558696", "558697", "558698", "558699", "558700", "558701", "558702", "558703", "558704", "558705", "558706", "558707", "558708", "558709", "558710", "558711", "558712", "558713", "558714", "558716", "558717", "558718", "558719", "558720", "558721", "558722", "558723", "558724", "558725", "558726", "558727", "558728", "558729", "558730", "558731", "558732", "558733", "558734", "558735", "558736"), marketoUids);
        assertEquals(36, latestUids.size());
    }
}
