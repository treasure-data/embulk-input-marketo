package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ProgramInputPlugin extends MarketoBaseInputPluginDelegate<ProgramInputPlugin.PluginTask>
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
        @Config("query_by")
        @ConfigDefault("null")
        Optional<QueryBy> getQueryBy();

        @Config("tag_type")
        @ConfigDefault("null")
        Optional<String> getTagType();

        @Config("tag_value")
        @ConfigDefault("null")
        Optional<String> getTagVallue();

        @Config("earliest_updated_at")
        @ConfigDefault("null")
        Optional<Date> getEarliestUpdatedAt();

        @Config("latest_updated_at")
        @ConfigDefault("null")
        Optional<Date> getLatestUpdatedAt();

        @Config("filter_type")
        @ConfigDefault("null")
        Optional<String> getFilterType();

        @Config("filter_values")
        @ConfigDefault("null")
        Optional<List<String>> getFilterValues();

        @Config("report_duration")
        @ConfigDefault("null")
        Optional<Long> getReportDuration();

        void setLatestUpdatedAt(Optional<Date> latestUpdatedAt);
    }

    public ProgramInputPlugin()
    {
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        // validate if query_by is selected
        if (task.getQueryBy().isPresent()) {
            switch(task.getQueryBy().get()) {
            case TAG_TYPE:
                //make sure tag type and tag value are not empty
                if (!task.getTagType().isPresent() || !task.getTagVallue().isPresent()) {
                    throw new ConfigException("tag_type and tag_value are required when query by Tag Type");
                }
                break;
            case DATE_RANGE:
                // make sure earliest_updated_at is not empty
                if (!task.getEarliestUpdatedAt().isPresent()) {
                    throw new ConfigException("`earliest_updated_at` is required when query by Date Range");
                }

                DateTime earliest = new DateTime(task.getEarliestUpdatedAt().get());
                if (task.getReportDuration().isPresent()) {
                    logger.info("`report_duration` is present, Prefer `report_duration` over `latest_updated_at`");
                    // Update the latestUpdatedAt for the config
                    DateTime latest = earliest.plus(task.getReportDuration().get());
                    task.setLatestUpdatedAt(Optional.of(latest.toDate()));
                }

                // latest_updated_at is required calculate time range
                if (!task.getLatestUpdatedAt().isPresent()) {
                    throw new ConfigException("`latest_updated_at` is required when query by Date Range");
                }

                DateTime latest = new DateTime(task.getLatestUpdatedAt().get());
                if (earliest.isAfter(DateTime.now())) {
                    throw new ConfigException(String.format("`earliest_updated_at` (%s) cannot precede the current date (%s)",
                                    earliest.toString(DATE_FORMATTER),
                                    (DateTime.now().toString(DATE_FORMATTER))));
                }

                if (earliest.isAfter(latest)) {
                    throw new ConfigException(String.format("Invalid date range. `earliest_updated_at` (%s) cannot precede the `latest_updated_at` (%s).",
                                    earliest.toString(DATE_FORMATTER),
                                    latest.toString(DATE_FORMATTER)));
                }
                // if filter type is selected, filter value must be presented
                if (task.getFilterType().isPresent() && (!task.getFilterValues().isPresent() || task.getFilterValues().get().isEmpty())) {
                    throw new ConfigException("filter_value is required when selected filter_type");
                }
            }
        }
    }

    @Override
    public TaskReport ingestServiceData(PluginTask task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        // query by date range and incremental import and not preview
        if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.DATE_RANGE && task.getIncremental() && !Exec.isPreview()) {
            DateTime latestUpdateAt = new DateTime(task.getLatestUpdatedAt().get());
            DateTime now = DateTime.now();
            // Do not run incremental import if latest_updated_at precede current time
            if (latestUpdateAt.isAfter(now)) {
                logger.warn("`latest_updated_at` ({}) preceded current time ({}). Will try to import next run", latestUpdateAt.toString(DATE_FORMATTER), now.toString(DATE_FORMATTER));

                TaskReport taskReport = Exec.newTaskReport();
                taskReport.set("earliest_updated_at", new DateTime(task.getEarliestUpdatedAt().get()).toString(DATE_FORMATTER));
                if (task.getReportDuration().isPresent()) {
                    taskReport.set("report_duration", task.getReportDuration().get());
                }
                return taskReport;
            }
        }
        return super.ingestServiceData(task, recordImporter, taskIndex, pageBuilder);
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        Iterable<ObjectNode> nodes = null;
        if (task.getQueryBy().isPresent()) {
            switch (task.getQueryBy().get()) {
            case TAG_TYPE:
                nodes = marketoService.getProgramsByTag(task.getTagType().get(), task.getTagVallue().get());
                break;
            case DATE_RANGE:
                nodes = marketoService.getProgramsByDateRange(task.getEarliestUpdatedAt().get(),
                                task.getLatestUpdatedAt().get(),
                                task.getFilterType().orNull(),
                                task.getFilterValues().orNull());
            }
        }
        else {
            nodes = marketoService.getPrograms();
        }
        return FluentIterable.from(nodes).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        ConfigDiff configDiff = super.buildConfigDiff(task, schema, taskCount, taskReports);
        // set next next earliestUpdatedAt, latestUpdatedAt
        if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.DATE_RANGE && task.getIncremental()) {
            DateTime earliest = new DateTime(task.getEarliestUpdatedAt().get());
            DateTime latest = new DateTime(task.getLatestUpdatedAt().get());

            Duration d = new Duration(earliest, latest);
            DateTime nextEarliestUpdatedAt = latest.plusSeconds(1);

            configDiff.set("earliest_updated_at", nextEarliestUpdatedAt.toString(DATE_FORMATTER));
            configDiff.set("report_duration", task.getReportDuration().or(d.getMillis()));
        }
        return configDiff;
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        builder.add("id", Types.LONG)
                .add("name", Types.STRING)
                .add("sfdcId", Types.STRING)
                .add("sfdcName", Types.STRING)
                .add("description", Types.STRING)
                .add("createdAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("updatedAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("startDate", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("endDate", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("url", Types.STRING)
                .add("type", Types.STRING)
                .add("channel", Types.STRING)
                .add("folder", Types.JSON)
                .add("status", Types.STRING)
                .add("costs", Types.JSON)
                .add("tags", Types.JSON)
                .add("workspace", Types.STRING);
        return builder.build();
    }

    public enum QueryBy {
        TAG_TYPE,
        DATE_RANGE;

        @JsonCreator
        public static QueryBy of(String value)
        {
            return QueryBy.valueOf(value.toUpperCase());
        }
    }
}
