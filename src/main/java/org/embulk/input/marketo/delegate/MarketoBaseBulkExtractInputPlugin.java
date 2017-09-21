package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.LineDecoder;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tai.khuu on 9/18/17.
 */
public abstract class MarketoBaseBulkExtractInputPlugin<T extends MarketoBaseBulkExtractInputPlugin.PluginTask> extends MarketoBaseInputPluginDelegate<T>
{
    private static final Logger LOGGER = Exec.getLogger(MarketoBaseBulkExtractInputPlugin.class);

    private static final String LATEST_FETCH_TIME = "latest_fetch_time";

    private static final String LATEST_UID_LIST = "latest_uids";

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, CsvTokenizer.PluginTask
    {
        @Config("from_date")
        @ConfigDefault("null")
        Optional<Date> getFromDate();

        @Config("fetch_days")
        @ConfigDefault("1")
        Integer getFetchDays();

        @Config("latest_fetch_time")
        @ConfigDefault("null")
        Optional<Long> getLatestFetchTime();

        @ConfigInject
        BufferAllocator getBufferAllocator();

        @Config("polling_interval_second")
        @ConfigDefault("60")
        Integer getPollingIntervalSecond();

        @Config("bulk_job_timeout_second")
        @ConfigDefault("3600")
        Integer getBulkJobTimeoutSecond();

        @Config("latest_uids")
        @ConfigDefault("[]")
        Set<String> previousUIds();
    }

    private String incrementalColumn;

    private String uidColumn;

    public MarketoBaseBulkExtractInputPlugin(String incrementalColumn, String uidColumn)
    {
        this.incrementalColumn = incrementalColumn;
        this.uidColumn = uidColumn;
    }

    @Override
    public void validateInputTask(T task)
    {
        if (!task.getFromDate().isPresent()) {
            throw new ConfigException("From date is required for Bulk Extract");
        }
        super.validateInputTask(task);
    }

    @Override
    public ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        ConfigDiff configDiff = super.buildConfigDiff(task, schema, taskCount, taskReports);
        Long currentLatestFetchTime = 0L;
        Set<String> latestUIds = null;
        for (TaskReport taskReport : taskReports) {
            Long latestFetchTime = taskReport.get(Long.class, LATEST_FETCH_TIME);
            if (latestFetchTime == null) {
                continue;
            }
            if (currentLatestFetchTime < latestFetchTime) {
                currentLatestFetchTime = latestFetchTime;
                latestUIds = taskReport.get(Set.class, LATEST_UID_LIST);
            }
        }
        if (!currentLatestFetchTime.equals(0L)) {
            configDiff.set(LATEST_FETCH_TIME, currentLatestFetchTime);
            DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
            configDiff.set("from_date", df.format(new Date(currentLatestFetchTime)));
            configDiff.set(LATEST_UID_LIST, latestUIds);
        }
        return configDiff;
    }

    @Override
    public TaskReport ingestServiceData(T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        InputStream extractedStream = getExtractedStream(task, pageBuilder.getSchema());
        return importRecordFromFile(task, extractedStream, recordImporter, pageBuilder);
    }

    protected TaskReport importRecordFromFile(T task, InputStream inputStream, RecordImporter recordImporter, PageBuilder pageBuilder)
    {
        Set<String> latestUids = task.previousUIds();
        TaskReport taskReport = Exec.newTaskReport();
        int imported = 0;
        Timestamp currentTimestamp = null;
        if (task.getLatestFetchTime().isPresent()) {
            currentTimestamp = Timestamp.ofEpochMilli(task.getLatestFetchTime().get());
        }
        TimestampParser timestampParser = new TimestampParser(MarketoUtils.ISO_8601_FORMAT, DateTimeZone.UTC);
        try (LineDecoder lineDecoder = new LineDecoder(new InputStreamFileInput(task.getBufferAllocator(), inputStream), task)) {
            CsvTokenizer csvTokenizer = new CsvTokenizer(lineDecoder, task);
            if (!csvTokenizer.nextFile()) {
                throw new DataException("Can't read extract input stream");
            }
            csvTokenizer.nextRecord();
            List<String> headers = new ArrayList<>();
            while (csvTokenizer.hasNextColumn()) {
                headers.add(csvTokenizer.nextColumn());
            }
            while (csvTokenizer.nextRecord() && (imported < PREVIEW_RECORD_LIMIT || !Exec.isPreview())) {
                List<String> values = new ArrayList<>();
                while (csvTokenizer.hasNextColumn()) {
                    values.add(csvTokenizer.nextColumnOrNull());
                }
                final Map<String, String> kvMap = MarketoUtils.zip(headers, values);
                ObjectNode objectNode = MarketoUtils.transformToObjectNode(kvMap, pageBuilder.getSchema());
                if (!objectNode.has(incrementalColumn)) {
                    throw new DataException("Extracted record doesn't have incremental column " + incrementalColumn);
                }
                JsonNode incrementalJsonNode = objectNode.get(incrementalColumn);
                if (uidColumn != null) {
                    JsonNode uidField = objectNode.get(uidColumn);
                    String uid = uidField.asText();
                    if (latestUids.contains(uid)) {
                        //Duplicate value
                        continue;
                    }
                }
                String incrementalTimeStamp = incrementalJsonNode.asText();
                Timestamp timestamp = timestampParser.parse(incrementalTimeStamp);
                if (currentTimestamp == null) {
                    currentTimestamp = timestamp;
                }
                else {
                    int compareTo = currentTimestamp.compareTo(timestamp);
                    if (compareTo < 0) {
                        currentTimestamp = timestamp;
                        //switch timestamp
                        latestUids.clear();
                    }
                    else if (compareTo == 0) {
                        //timestamp is equal
                        if (uidColumn != null) {
                            JsonNode uidField = objectNode.get(uidColumn);
                            latestUids.add(uidField.asText());
                        }
                    }
                }
                recordImporter.importRecord(new JacksonServiceRecord(objectNode), pageBuilder);
                imported++;
            }
        }
        taskReport.set(LATEST_FETCH_TIME, currentTimestamp == null ? null : currentTimestamp.toEpochMilli());
        taskReport.set(LATEST_UID_LIST, latestUids);
        return taskReport;
    }
    protected abstract InputStream getExtractedStream(T task, Schema schema);
}
