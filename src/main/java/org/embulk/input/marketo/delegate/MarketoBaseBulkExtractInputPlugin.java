package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.bulk_extract.AllStringJacksonServiceRecord;
import org.embulk.input.marketo.bulk_extract.CsvRecordIterator;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.text.LineDecoder;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

/**
 * Created by tai.khuu on 9/18/17.
 */
public abstract class MarketoBaseBulkExtractInputPlugin<T extends MarketoBaseBulkExtractInputPlugin.PluginTask> extends MarketoBaseInputPluginDelegate<T>
{
    private static final String FROM_DATE = "from_date";

    private static final int MARKETO_MAX_RANGE_EXTRACT = 30;

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, CsvTokenizer.PluginTask
    {
        @Config("from_date")
        Date getFromDate();

        @Config("fetch_days")
        @ConfigDefault("1")
        Integer getFetchDays();

        @Config("latest_fetch_time")
        @ConfigDefault("null")
        Optional<Long> getLatestFetchTime();

        @Config("polling_interval_second")
        @ConfigDefault("60")
        Integer getPollingIntervalSecond();

        @Config("bulk_job_timeout_second")
        @ConfigDefault("3600")
        Integer getBulkJobTimeoutSecond();

        @Config("to_date")
        @ConfigDefault("null")
        Optional<Date> getToDate();

        void setToDate(Optional<Date> toDate);

        @Config("incremental_column")
        @ConfigDefault("\"createdAt\"")
        //Incremental column are only keep here since we don't want to introduce too much change to plugin
        //Consider remove it in next release
        Optional<String> getIncrementalColumn();

        void setIncrementalColumn(Optional<String> incrementalColumn);

        @Config("uid_column")
        @ConfigDefault("null")
        Optional<String> getUidColumn();
        void setUidColumn(Optional<String> uidColumn);
    }

    @Override
    public void validateInputTask(T task)
    {
        super.validateInputTask(task);
        if (task.getFromDate() == null) {
            throw new ConfigException("From date is required for Bulk Extract");
        }
        if (task.getFromDate().getTime() >= OffsetDateTime.parse(task.getJobStartTime(), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli()) {
            throw new ConfigException("From date can't not be in future");
        }
        if (task.getIncremental()
                && task.getIncrementalColumn().isPresent()
                && task.getIncrementalColumn().get().equals("updatedAt")) {
            throw new ConfigException("Column 'updatedAt' cannot be incremental imported");
        }
        //Calculate to date
        OffsetDateTime toDate = getToDate(task);
        task.setToDate(Optional.of(Date.from(toDate.toInstant())));
    }

    public OffsetDateTime getToDate(T task)
    {
        Date fromDate = task.getFromDate();
        final OffsetDateTime jobStartTime = OffsetDateTime.parse(task.getJobStartTime(), DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        OffsetDateTime dateTime = OffsetDateTime.ofInstant(fromDate.toInstant(), ZoneOffset.UTC);
        OffsetDateTime toDate = dateTime.plusDays(task.getFetchDays());
        if (toDate.isAfter(jobStartTime)) {
            //Lock down to date
            toDate = jobStartTime;
        }
        return toDate;
    }

    @Override
    public ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        ConfigDiff configDiff = super.buildConfigDiff(task, schema, taskCount, taskReports);
        String incrementalColumn = task.getIncrementalColumn().orElse(null);
        if (incrementalColumn != null && task.getIncremental()) {
            DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
            // We will always move the range forward.
            Date toDate = task.getToDate().orElse(null);
            configDiff.set(FROM_DATE, df.format(toDate));
        }
        return configDiff;
    }

    @Override
    public TaskReport ingestServiceData(final T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (Exec.isPreview()) {
            return MarketoUtils.importMockPreviewData(pageBuilder, PREVIEW_RECORD_LIMIT);
        }
        else {
            try (LineDecoderIterator decoderIterator = getLineDecoderIterator(task)) {
                Iterator<Map<String, String>> csvRecords = Iterators.concat(Iterators.transform(decoderIterator,
                        (Function<LineDecoder, Iterator<Map<String, String>>>) input -> new CsvRecordIterator(input, task)));
                //Keep the preview code here when we can enable real preview
                if (Exec.isPreview()) {
                    csvRecords = Iterators.limit(csvRecords, PREVIEW_RECORD_LIMIT);
                }
                int imported = 0;
                while (csvRecords.hasNext()) {
                    Map<String, String> csvRecord = csvRecords.next();
                    ObjectNode objectNode = MarketoUtils.OBJECT_MAPPER.valueToTree(csvRecord);
                    recordImporter.importRecord(new AllStringJacksonServiceRecord(objectNode), pageBuilder);
                    imported = imported + 1;
                }
                return taskReport;
            }
        }
    }

    private LineDecoderIterator getLineDecoderIterator(T task)
    {
        final OffsetDateTime fromDate = OffsetDateTime.ofInstant(task.getFromDate().toInstant(), ZoneOffset.UTC);
        final OffsetDateTime toDate = task.getToDate().isPresent() ?
                OffsetDateTime.ofInstant(task.getToDate().get().toInstant(), ZoneOffset.UTC) :
                OffsetDateTime.now(ZoneOffset.UTC);
        List<MarketoUtils.DateRange> dateRanges = MarketoUtils.sliceRange(fromDate, toDate, MARKETO_MAX_RANGE_EXTRACT);
        final Iterator<MarketoUtils.DateRange> iterator = dateRanges.iterator();
        return new LineDecoderIterator(iterator, task);
    }

    @Override
    protected final Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, T task)
    {
        throw new UnsupportedOperationException();
    }

    protected abstract InputStream getExtractedStream(MarketoService service, T task, OffsetDateTime fromDate, OffsetDateTime toDate);

    private final class LineDecoderIterator implements Iterator<LineDecoder>, AutoCloseable
    {
        private LineDecoder currentLineDecoder;

        private final Iterator<MarketoUtils.DateRange> dateRangeIterator;

        private final MarketoService marketoService;

        private final MarketoRestClient marketoRestClient;
        private final T task;
        public LineDecoderIterator(Iterator<MarketoUtils.DateRange> dateRangeIterator, T task)
        {
            marketoRestClient = createMarketoRestClient(task);
            marketoService = new MarketoServiceImpl(marketoRestClient);
            this.dateRangeIterator = dateRangeIterator;
            this.task = task;
        }

        @Override
        public void close()
        {
            if (currentLineDecoder != null) {
                currentLineDecoder.close();
            }
            if (marketoRestClient != null) {
                marketoRestClient.close();
            }
        }

        @Override
        public boolean hasNext()
        {
            return dateRangeIterator.hasNext();
        }

        @Override
        public LineDecoder next()
        {
            if (hasNext()) {
                MarketoUtils.DateRange next = dateRangeIterator.next();
                InputStream extractedStream = getExtractedStream(marketoService, task, next.fromDate, next.toDate);
                currentLineDecoder = LineDecoder.of(new InputStreamFileInput(Exec.getBufferAllocator(), extractedStream), StandardCharsets.UTF_8, null);
                return currentLineDecoder;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Removed are not supported");
        }
    }
}
