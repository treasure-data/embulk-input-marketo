package org.embulk.input.marketo.delegate;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoInputPluginDelegate;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.embulk.input.marketo.delegate.MarketoBaseBulkExtractInputPlugin.PluginTask;
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

    private MarketoBaseBulkExtractInputPlugin<PluginTask> baseBulkExtractInputPlugin;
    private PluginTask validBaseTask;

    @Before
    public void prepare() throws IOException
    {
        baseBulkExtractInputPlugin = Mockito.mock(MarketoBaseBulkExtractInputPlugin.class, Mockito.CALLS_REAL_METHODS);
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        ConfigSource configSource = configLoader.fromYaml(
                this.getClass().getResourceAsStream("/config/activity_bulk_extract_config.yaml"));
        validBaseTask = CONFIG_MAPPER.map(configSource, PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void validateInputTaskError()
    {
        PluginTask pluginTask = Mockito.mock(PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(null);
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
    }

    @Test(expected = ConfigException.class)
    public void invalidInputTaskWhenIncrementalByUpdatedAt()
    {
        PluginTask task = mock(PluginTask.class, delegatesTo(validBaseTask));
        when(task.getIncrementalColumn()).thenReturn(Optional.of("updatedAt"));
        when(task.getIncremental()).thenReturn(true);
        baseBulkExtractInputPlugin.validateInputTask(task);
    }

    @Test
    public void validInputTaskWhenIncrementalOtherThanUpdatedAt()
    {
        PluginTask task = mock(PluginTask.class, delegatesTo(validBaseTask));
        when(task.getIncremental()).thenReturn(true);
        when(task.getIncrementalColumn()).thenReturn(Optional.of("anythingButUpdatedAt"));
        baseBulkExtractInputPlugin.validateInputTask(task);  // should not throw
    }

    @Test
    public void validInputTaskWhenNonIncrementalWhileSetUpdatedAt()
    {
        PluginTask task = mock(PluginTask.class, delegatesTo(validBaseTask));
        when(task.getIncremental()).thenReturn(false);
        when(task.getIncrementalColumn()).thenReturn(Optional.of("updatedAt"));
        baseBulkExtractInputPlugin.validateInputTask(task);  // should not throw
    }

    @Test()
    public void validateInputTaskToDateLessThanJobStartTime()
    {
        Date fromDate = new Date(1504224000000L);
        OffsetDateTime jobStartTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1506842144000L), ZoneOffset.UTC);
        PluginTask pluginTask = Mockito.mock(PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
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
        OffsetDateTime jobStartTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1506842144000L), ZoneOffset.UTC);
        PluginTask pluginTask = Mockito.mock(PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

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
        OffsetDateTime jobStartTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1504396800000L), ZoneOffset.UTC);
        PluginTask pluginTask = Mockito.mock(PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(fromDate);
        Mockito.when(pluginTask.getFetchDays()).thenReturn(7);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        baseBulkExtractInputPlugin.validateInputTask(pluginTask);
        ArgumentCaptor<Optional<Date>> toDateArgumentCaptor = ArgumentCaptor.forClass(Optional.class);
        Mockito.verify(pluginTask, Mockito.times(1)).setToDate(toDateArgumentCaptor.capture());
        assertEquals(jobStartTime.toInstant().toEpochMilli(), toDateArgumentCaptor.getValue().get().getTime());
    }

    @Test
    public void getToDate()
    {
        OffsetDateTime date = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1505033728000L), ZoneOffset.UTC);
        OffsetDateTime jobStartTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(1507625728000L), ZoneOffset.UTC);
        PluginTask pluginTask = Mockito.mock(MarketoInputPluginDelegate.PluginTask.class);
        Mockito.when(pluginTask.getFromDate()).thenReturn(Date.from(date.toInstant()));
        Mockito.when(pluginTask.getFetchDays()).thenReturn(30);
        Mockito.when(pluginTask.getJobStartTime()).thenReturn(jobStartTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        OffsetDateTime toDate = baseBulkExtractInputPlugin.getToDate(pluginTask);
        assertEquals(toDate, jobStartTime);
    }

    @Test
    public void buildConfigDiff()
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
