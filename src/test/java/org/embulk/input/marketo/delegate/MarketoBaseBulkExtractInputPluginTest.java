package org.embulk.input.marketo.delegate;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by khuutantaitai on 10/3/17.
 */
public class MarketoBaseBulkExtractInputPluginTest
{
    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private MarketoBaseBulkExtractInputPlugin<MarketoBaseBulkExtractInputPlugin.PluginTask> baseBulkExtractInputPlugin;
    private MarketoBaseBulkExtractInputPlugin.PluginTask validBaseTask;

    @Before
    public void prepare() throws IOException
    {
        baseBulkExtractInputPlugin = Mockito.mock(MarketoBaseBulkExtractInputPlugin.class, Mockito.CALLS_REAL_METHODS);
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        ConfigSource configSource = configLoader.fromYaml(
                this.getClass().getResourceAsStream("/config/activity_bulk_extract_config.yaml"));
        validBaseTask = configSource.loadConfig(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void validateInputTaskError()
    {
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = Mockito.mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(null);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
    }

    @Test()
    public void validateInputTaskToDateLessThanJobStartTime()
    {
        Date fromDate = new Date(1504224000000L);
        DateTime jobStartTime = new DateTime(1506842144000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = Mockito.mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        Mockito.when(pluginTask.getFetchDays()).thenReturn(7);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Optional<Date>> argumentCaptor = ArgumentCaptor.forClass(Optional.class);
        Mockito.verify(pluginTask, Mockito.times(1)).setToDate(argumentCaptor.capture());
        assertEquals(1504828800000L, argumentCaptor.getValue().get().getTime());
    }

    @Test()
    public void validateInputTaskFromDateMoreThanJobStartTime()
    {
        Date fromDate = new Date(1507619744000L);
        DateTime jobStartTime = new DateTime(1506842144000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = Mockito.mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);

        try {
            baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        }
        catch (ConfigException ex) {
            return;
        }
        fail();
    }

    @Test()
    public void validateInputTaskToDateMoreThanJobStartTime()
    {
        Date fromDate = new Date(1504224000000L);
        DateTime jobStartTime = new DateTime(1504396800000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = Mockito.mock(MarketoBaseBulkExtractInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getFetchDays()).thenReturn(7);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Optional<Date>> toDateArgumentCaptor = ArgumentCaptor.forClass(Optional.class);
        Mockito.verify(pluginTask, Mockito.times(1)).setToDate(toDateArgumentCaptor.capture());
        assertEquals(jobStartTime.getMillis(), toDateArgumentCaptor.getValue().get().getTime());
    }

    @Test
    public void getToDate() throws Exception
    {
        DateTime date = new DateTime(1505033728000L);
        DateTime jobStartTime = new DateTime(1507625728000L);
        MarketoBaseBulkExtractInputPlugin.PluginTask pluginTask = Mockito.mock(MarketoInputPluginDelegate.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(date.toDate());
        Mockito.when(pluginTask.getFetchDays()).thenReturn(30);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime);
        DateTime toDate = baseBulkExtractInputPlugin.getToDate(pluginTask);
        assertEquals(toDate, jobStartTime);
    }

    @Test
    public void buildConfigDiff() throws Exception
    {
        TaskReport taskReport1 = Mockito.mock(TaskReport.class);
        MarketoInputPluginDelegate.PluginTask task = Mockito.mock(MarketoInputPluginDelegate.PluginTask.class);
        Mockito.when(task.getIncremental()).thenReturn(true);
        Mockito.when(task.getIncrementalColumn()).thenReturn(Optional.of("createdAt"));
        Date toDate = new Date(1507625728000L);
        Mockito.when(task.getToDate()).thenReturn(Optional.of(toDate));
        ConfigDiff configDiff = baseBulkExtractInputPlugin.buildConfigDiff(task, Mockito.mock(Schema.class), 1, Arrays.asList(taskReport1));
        DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        assertEquals(df.format(toDate), configDiff.get(String.class, "from_date"));
    }
}
