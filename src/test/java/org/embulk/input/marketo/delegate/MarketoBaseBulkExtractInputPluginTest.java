package org.embulk.input.marketo.delegate;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoInputPluginDelegate;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.Schema;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoBaseBulkExtractInputPluginTest
{
    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private MarketoBaseBulkExtractInputPlugin<MarketoBaseBulkExtractInputPlugin.PluginTask> baseBulkExtractInputPlugin;

    @Before
    public void prepare()
    {
        baseBulkExtractInputPlugin = mock(MarketoBaseBulkExtractInputPlugin.class, Mockito.CALLS_REAL_METHODS);
    }

    @Test(expected = ConfigException.class)
    public void validateInputTaskError()
    {
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(null);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
    }

    @Test()
    public void validateInputTaskToDateLessThanJobStartTime()
    {
        Date fromDate = new Date(1504224000000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(fromDate);
        when(pluginTask.getFetchDays()).thenReturn(7);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Optional<Date>> argumentCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(pluginTask, times(1)).setToDate(argumentCaptor.capture());
        assertEquals(1504828800000L, argumentCaptor.getValue().get().getTime());
    }

    @Test()
    public void validateInputTaskToDateMoreThanJobStartTime()
    {
        Date fromDate = new Date(1504224000000L);
        DateTime jobStartTime = new DateTime(1504396800000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(fromDate);
        when(pluginTask.getFetchDays()).thenReturn(7);
        when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Optional<Date>> toDateArgumentCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(pluginTask, times(1)).setToDate(toDateArgumentCaptor.capture());
        assertEquals(jobStartTime.minusHours(1).getMillis(), toDateArgumentCaptor.getValue().get().getTime());
    }

    @Test
    public void getToDate() throws Exception
    {
        DateTime date = new DateTime(1505033728000L);
        DateTime jobStartTime = new DateTime(1507625728000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = mock(MarketoInputPluginDelegate.PluginTask.class);
        when(pluginTask.getFromDate()).thenReturn(date.toDate());
        when(pluginTask.getFetchDays()).thenReturn(30);
        when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        DateTime toDate = baseBulkExtractInputPlugin.getToDate(pluginTask);
        assertEquals(toDate, jobStartTime);
    }

    @Test
    public void buildConfigDiff() throws Exception
    {
        TaskReport taskReport1 = mock(TaskReport.class);
        TaskReport taskReport2 = mock(TaskReport.class);
        when(taskReport1.get(Set.class, "latest_uids")).thenReturn(Sets.newHashSet("id1", "id2"));
        when(taskReport2.get(Set.class, "latest_uids")).thenReturn(Sets.newHashSet("id3", "id4"));
        when(taskReport1.get(Long.class, "latest_fetch_time")).thenReturn(1507539328000L);
        when(taskReport2.get(Long.class, "latest_fetch_time")).thenReturn(1507625728000L);
        MarketoInputPluginDelegate.PluginTask task = mock(MarketoInputPluginDelegate.PluginTask.class);
        when(task.getIncremental()).thenReturn(true);
        when(task.getIncrementalColumn()).thenReturn(Optional.of("createdAt"));
        Date toDate = new Date(1507625728000L);
        when(task.getToDate()).thenReturn(Optional.of(toDate));
        ConfigDiff configDiff = baseBulkExtractInputPlugin.buildConfigDiff(task, mock(Schema.class), 1, Arrays.asList(taskReport1, taskReport2));
        long latestFetchTime = configDiff.get(Long.class, "latest_fetch_time");
        assertEquals(1507625728000L, latestFetchTime);
        assertEquals(Sets.newHashSet("id3", "id4"), configDiff.get(Set.class, "latest_uids"));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        assertEquals(df.format(toDate), configDiff.get(String.class, "from_date"));
    }
}
