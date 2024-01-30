package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.file.FileInputInputStream;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.text.LineDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

/**
 * Created by tai.khuu on 9/18/17.
 */
public abstract class MarketoBaseBulkExtractInputPlugin<T extends MarketoBaseBulkExtractInputPlugin.PluginTask> extends MarketoBaseInputPluginDelegate<T>
{
    private static final String FROM_DATE = "from_date";

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketoBaseBulkExtractInputPlugin.class);

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
                int imported = 0;
                while (decoderIterator.hasNext()) {
                    try {
                        CSVParser csvParser = getCsvParser(decoderIterator);
                        System.gc();
                        while (csvParser.iterator().hasNext()) {
                            CSVRecord csvRecord = csvParser.iterator().next();
                            ObjectNode objectNode = MarketoUtils.OBJECT_MAPPER.valueToTree(csvRecord.toMap());
                            recordImporter.importRecord(new AllStringJacksonServiceRecord(objectNode), pageBuilder);
                            if (csvParser.getRecordNumber() % 10000 == 0) {
                                LOGGER.info("transfer record count: " + csvParser.getRecordNumber());
                            }
                        }
                        LOGGER.info("transfer record count: " + csvParser.getRecordNumber());
                        csvParser.close();
                    }
                    catch (CsvTokenizer.InvalidValueException | IllegalArgumentException | IOException ex) {
                        LOGGER.warn("skipped csv line: " + ExceptionUtils.getStackTrace(ex));
                    }
                    imported = imported + 1;
                }
                return taskReport;
            }
        }
    }

    private CSVParser getCsvParser(LineDecoderIterator decoderIterator) throws IOException
    {
        Reader inputStream = decoderIterator.next();
        CsvTokenizer csvTokenizer = new CsvTokenizer(inputStream);
        return csvTokenizer.csvParse();
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

    private final class LineDecoderIterator implements Iterator<Reader>, AutoCloseable
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
        public Reader next()
        {
            if (hasNext()) {
                MarketoUtils.DateRange next = dateRangeIterator.next();
                InputStream extractedStream = getExtractedStream(marketoService, task, next.fromDate, next.toDate);
                InputStreamFileInput in = new InputStreamFileInput(Exec.getBufferAllocator(), extractedStream);
                FileInputInputStream fileInputInputStream = new FileInputInputStream(in);

                CharsetDecoder decoder = task.getCharset().newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                        .onUnmappableCharacter(CodingErrorAction.REPLACE);
                BufferedReader b = new BufferedReader(new InputStreamReader(fileInputInputStream, decoder));
                fileInputInputStream.nextFile();

                return b;
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
