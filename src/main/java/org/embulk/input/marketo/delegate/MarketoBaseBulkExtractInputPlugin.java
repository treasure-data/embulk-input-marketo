package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceValue;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.LineDecoder;
import org.joda.time.DateTime;
import org.msgpack.value.Value;

import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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

        @ConfigInject
        BufferAllocator getBufferAllocator();

        @Config("polling_interval_second")
        @ConfigDefault("60")
        Integer getPollingIntervalSecond();

        @Config("bulk_job_timeout_second")
        @ConfigDefault("3600")
        Integer getBulkJobTimeoutSecond();

        @Config("incremental")
        @ConfigDefault("true")
        Boolean getIncremental();

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
        if (task.getFromDate().getTime() >= task.getJobStartTime().getMillis()) {
            throw new ConfigException("From date can't not be in future");
        }
        if (task.getIncremental()
                && task.getIncrementalColumn().isPresent()
                && task.getIncrementalColumn().get().equals("updatedAt")) {
            throw new ConfigException("Column 'updatedAt' cannot be incremental imported");
        }
        //Calculate to date
        DateTime toDate = getToDate(task);
        task.setToDate(Optional.of(toDate.toDate()));
    }

    public DateTime getToDate(T task)
    {
        Date fromDate = task.getFromDate();
        DateTime dateTime = new DateTime(fromDate);
        DateTime toDate = dateTime.plusDays(task.getFetchDays());
        if (toDate.isAfter(task.getJobStartTime())) {
            //Lock down to date
            toDate = task.getJobStartTime();
        }
        return toDate;
    }

    @Override
    public ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        ConfigDiff configDiff = super.buildConfigDiff(task, schema, taskCount, taskReports);
        String incrementalColumn = task.getIncrementalColumn().orNull();
        if (incrementalColumn != null && task.getIncremental()) {
            DateFormat df = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
            // We will always move the range forward.
            Date toDate = task.getToDate().orNull();
            configDiff.set(FROM_DATE, df.format(toDate));
        }
        return configDiff;
    }

    @Override
    public TaskReport ingestServiceData(final T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        TaskReport taskReport = Exec.newTaskReport();
        if (Exec.isPreview()) {
            return importMockPreviewData(pageBuilder);
        }
        else {
            try (LineDecoderIterator decoderIterator = getLineDecoderIterator(task)) {
                Iterator<Map<String, String>> csvRecords = Iterators.concat(Iterators.transform(decoderIterator, new Function<LineDecoder, Iterator<Map<String, String>>>()
                {
                    @Override
                    public Iterator<Map<String, String>> apply(LineDecoder input)
                    {
                        return new CsvRecordIterator(input, task);
                    }
                }));
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

    /**
     * This method should be removed when we allow skip preview phase
     * @param pageBuilder
     * @return TaskReport
     */
    private TaskReport importMockPreviewData(final PageBuilder pageBuilder)
    {
        final JsonParser jsonParser = new JsonParser();
        Schema schema = pageBuilder.getSchema();
        for (int i = 1; i <= PREVIEW_RECORD_LIMIT; i++) {
            final int rowNum = i;
            schema.visitColumns(new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    pageBuilder.setBoolean(column, false);
                }

                @Override
                public void longColumn(Column column)
                {
                    pageBuilder.setLong(column, 12345L);
                }

                @Override
                public void doubleColumn(Column column)
                {
                    pageBuilder.setDouble(column, 12345.123);
                }

                @Override
                public void stringColumn(Column column)
                {
                    pageBuilder.setString(column, column.getName() + "_" + rowNum);
                }

                @Override
                public void timestampColumn(Column column)
                {
                    pageBuilder.setTimestamp(column, Timestamp.ofEpochMilli(System.currentTimeMillis()));
                }

                @Override
                public void jsonColumn(Column column)
                {
                    pageBuilder.setJson(column, jsonParser.parse("{\"mockKey\":\"mockValue\"}"));
                }
            });
            pageBuilder.addRecord();
        }
        return Exec.newTaskReport();
    }

    private LineDecoderIterator getLineDecoderIterator(T task)
    {
        List<MarketoUtils.DateRange> dateRanges = MarketoUtils.sliceRange(new DateTime(task.getFromDate()), new DateTime(task.getToDate().orNull()), MARKETO_MAX_RANGE_EXTRACT);
        final Iterator<MarketoUtils.DateRange> iterator = dateRanges.iterator();
        return new LineDecoderIterator(iterator, task);
    }

    @Override
    protected final Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, T task)
    {
        throw new UnsupportedOperationException();
    }

    protected abstract InputStream getExtractedStream(MarketoService service, T task, DateTime fromDate, DateTime toDate);

    private static class AllStringJacksonServiceRecord extends JacksonServiceRecord
    {
        public AllStringJacksonServiceRecord(ObjectNode record)
        {
            super(record);
        }

        @Override
        public JacksonServiceValue getValue(ValueLocator locator)
        {
            // We know that this thing only contain text.
            JacksonServiceValue value = super.getValue(locator);
            return new StringConverterJacksonServiceRecord(value.stringValue());
        }
    }

    private static class StringConverterJacksonServiceRecord extends JacksonServiceValue
    {
        private String textValue;

        public StringConverterJacksonServiceRecord(String textValue)
        {
            super(null);
            this.textValue = textValue;
        }

        @Override
        public boolean isNull()
        {
            return textValue == null || textValue.equals("null");
        }

        @Override
        public boolean booleanValue()
        {
            return Boolean.parseBoolean(textValue);
        }

        @Override
        public double doubleValue()
        {
            return Double.parseDouble(textValue);
        }

        @Override
        public Value jsonValue(JsonParser jsonParser)
        {
            return jsonParser.parse(textValue);
        }

        @Override
        public long longValue()
        {
            return Long.parseLong(textValue);
        }

        @Override
        public String stringValue()
        {
            return textValue;
        }

        @Override
        public Timestamp timestampValue(TimestampParser timestampParser)
        {
            return timestampParser.parse(textValue);
        }
    }

    private final class LineDecoderIterator implements Iterator<LineDecoder>, AutoCloseable
    {
        private LineDecoder currentLineDecoder;

        private Iterator<MarketoUtils.DateRange> dateRangeIterator;

        private MarketoService marketoService;

        private MarketoRestClient marketoRestClient;
        private T task;
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
                currentLineDecoder = new LineDecoder(new InputStreamFileInput(task.getBufferAllocator(), extractedStream), task);
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

    private class CsvRecordIterator implements Iterator<Map<String, String>>
    {
        private CsvTokenizer tokenizer;

        private List<String> headers;

        private Map<String, String> currentCsvRecord;
        public CsvRecordIterator(LineDecoder lineDecoder, T task)
        {
            tokenizer = new CsvTokenizer(lineDecoder, task);
            if (!tokenizer.nextFile()) {
                throw new DataException("Can't read extract input stream");
            }
            headers = new ArrayList<>();
            tokenizer.nextRecord();
            while (tokenizer.hasNextColumn()) {
                headers.add(tokenizer.nextColumn());
            }
        }

        @Override
        public boolean hasNext()
        {
            if (currentCsvRecord == null) {
                currentCsvRecord = getNextCSVRecord();
            }
            return currentCsvRecord != null;
        }

        @Override
        public Map<String, String> next()
        {
            try {
                if (hasNext()) {
                    return currentCsvRecord;
                }
            }
            finally {
                currentCsvRecord = null;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
        private Map<String, String> getNextCSVRecord()
        {
            if (!tokenizer.nextRecord()) {
                return null;
            }
            Map<String, String> kvMap = new HashMap<>();
            try {
                int i = 0;
                while (tokenizer.hasNextColumn()) {
                    kvMap.put(headers.get(i), tokenizer.nextColumnOrNull());
                    i++;
                }
            }
            catch (CsvTokenizer.InvalidValueException ex) {
                throw new DataException("Encounter exception when parse csv file. Please check to see if you are using the correct" +
                        "quote or escape character.", ex);
            }
            return kvMap;
        }
    }
}
