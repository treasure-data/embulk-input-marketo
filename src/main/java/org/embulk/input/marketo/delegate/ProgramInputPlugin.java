package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class ProgramInputPlugin extends MarketoBaseInputPluginDelegate<ProgramInputPlugin.PluginTask>
{
    private static final DateTimeFormatter DATE_FORMATER = DateTimeFormat.forPattern(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);

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

        @Config("incremental_import")
        @ConfigDefault("false")
        boolean getIncrementalImport();

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
        if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.TAG_TYPE) {
            //make sure tag type and tag value are not empty
            if (!task.getTagType().isPresent() || !task.getTagVallue().isPresent()) {
                throw new ConfigException("tag_type and tag_value are required when query by Tag Type");
            }
        }
        else if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.DATE_RANGE) {
            // make sure earliest_updated_at is not empty
            if (!task.getEarliestUpdatedAt().isPresent()) {
                throw new ConfigException("`earliest_updated_at` is required when query by Date Range");
            }
            DateTime earliest = new DateTime(task.getEarliestUpdatedAt().get());

            // If incremental report
            if (task.getIncrementalImport()) {
                // The very first run
                if (!task.getReportDuration().isPresent()) {
                    DateTime latest = new DateTime(task.getLatestUpdatedAt().get());
                    if (latest.isAfter(DateTime.now())) {
                        throw new ConfigException(String.format("`latest_updated_at` (%s) cannot precede the current date (%s) when incremental import",
                                        latest.toString(DATE_FORMATER),
                                        (DateTime.now().toString(DATE_FORMATER))));
                    }
                }
                else {
                    // Update the latestUpdatedAt for the config
                    DateTime latest = earliest.plus(task.getReportDuration().get());
                    // Only import until now
                    if (latest.isAfter(DateTime.now())) {
                        latest = DateTime.now();
                    }
                    task.setLatestUpdatedAt(Optional.of(latest.toDate()));
                }
            }

            // make sure latest_updated_at is not empty
            if (!task.getLatestUpdatedAt().isPresent()) {
                throw new ConfigException("`latest_updated_at` is required when query by Date Range");
            }

            DateTime latest = new DateTime(task.getLatestUpdatedAt().get());

            if (earliest.isAfter(DateTime.now())) {
                throw new ConfigException(String.format("`earliest_updated_at` (%s) cannot precede the current date (%s)",
                                earliest.toString(DATE_FORMATER),
                                (DateTime.now().toString(DATE_FORMATER))));
            }

            if (earliest.isAfter(latest)) {
                throw new ConfigException(String.format("Invalid date range. `earliest_updated_at` (%s) cannot precede the `latest_updated_at` (%s).",
                                earliest.toString(DATE_FORMATER),
                                latest.toString(DATE_FORMATER)));
            }
            // if filter type is selected, filter value must be presented
            if (task.getFilterType().isPresent() && (!task.getFilterValues().isPresent() || task.getFilterValues().get().isEmpty())) {
                throw new ConfigException("filter_value is required when selected filter_type");
            }
        }
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        Iterable<ObjectNode> nodes = null;
        if (!task.getQueryBy().isPresent()) {
            nodes = marketoService.getPrograms();
        }
        else if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.TAG_TYPE) {
            nodes = marketoService.getProgramsByTag(task.getTagType().get(), task.getTagVallue().get());
        }
        else if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.DATE_RANGE) {
            nodes = marketoService.getProgramsByDateRange(task.getEarliestUpdatedAt().get(),
                            task.getLatestUpdatedAt().get(),
                            task.getFilterType().orNull(),
                            task.getFilterValues().orNull());
        }

        return FluentIterable.from(nodes).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        ConfigDiff configDiff = super.buildConfigDiff(task, schema, taskCount, taskReports);
        // set next next earliestUpdatedAt, latestUpdatedAt
        if (task.getQueryBy().isPresent() && task.getQueryBy().get() == QueryBy.DATE_RANGE && task.getIncrementalImport()) {

            DateTime earliest = new DateTime(task.getEarliestUpdatedAt().get());
            DateTime latest = new DateTime(task.getLatestUpdatedAt().get());

            Duration d = new Duration(earliest, latest);
            DateTime nextEarliestUpdatedAt = latest.plusSeconds(1);

            configDiff.set("earliest_updated_at", nextEarliestUpdatedAt.toString(DATE_FORMATER));
            if (task.getReportDuration().isPresent()) {
                configDiff.set("report_duration", task.getReportDuration());
            }
            else {
                configDiff.set("report_duration", Optional.of(d.getMillis()));
            }
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
            if (value != null) {
                return QueryBy.valueOf(value.toUpperCase());
            }
            else {
                return null;
            }
        }
    }
}