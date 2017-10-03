package org.embulk.input.marketo.delegate;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoBaseBulkExtractInputPluginTest {

    private MarketoBaseBulkExtractInputPlugin<MarketoBaseBulkExtractInputPlugin.PluginTask> baseBulkExtractInputPlugin;

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    @Before
    public void prepare() {
        baseBulkExtractInputPlugin = mock(MarketoBaseBulkExtractInputPlugin.class,Mockito.CALLS_REAL_METHODS);
    }

    @Test(expected = ConfigException.class)
    public void validateInputTaskError() {
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(null);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
    }

    @Test()
    public void validateInputTaskToDateLessThanJobStartTime() {
        Date fromDate=new Date(1504224000000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(fromDate);
        when(pluginTask.getFetchDays()).thenReturn(7);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Date> argumentCaptor = ArgumentCaptor.forClass(Date.class);
        verify(pluginTask, times(1)).setToDate(argumentCaptor.capture());
        assertEquals(1504828800000L,argumentCaptor.getValue().getTime());
    }

    @Test()
    public void validateInputTaskToDateMoreThanJobStartTime() {
        Date fromDate = new Date(1504224000000L);
        DateTime jobStartTime = new DateTime(1504396800000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(fromDate);
        when(pluginTask.getFetchDays()).thenReturn(7);
        when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Date> toDateArgumentCaptor = ArgumentCaptor.forClass(Date.class);
        verify(pluginTask, times(1)).setToDate(toDateArgumentCaptor.capture());
        assertEquals(jobStartTime.minusHours(1).getMillis(), toDateArgumentCaptor.getValue().getTime());
    }

    @Test
    public void getToDate() throws Exception {
    }

    @Test
    public void buildConfigDiff() throws Exception {
    }

    @Test
    public void ingestServiceData() throws Exception {
    }

    @Test
    public void getInputStream() throws Exception {
    }

    @Test
    public void importRecordFromFile() throws Exception {
    }

    @Test
    public void getExtractedStream() throws Exception {
    }
}