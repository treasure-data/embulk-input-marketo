package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceRecord;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonTopLevelValueLocator;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.*;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class MarketoUtils
{
    public static final String MARKETO_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z";
    public static final String MARKETO_DATE_FORMAT = "%Y-%m-%d";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final Function<ObjectNode, ServiceRecord> TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION = new Function<ObjectNode, ServiceRecord>()
    {
        @Nullable
        @Override
        public JacksonServiceRecord apply(@Nullable ObjectNode input)
        {
            return new JacksonServiceRecord(input);
        }
    };

    public static final String MARKETO_DATE_SIMPLE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final String LIST_ID_COLUMN_NAME = "listId";

    public static final String PROGRAM_ID_COLUMN_NAME = "programId";

    private MarketoUtils()
    {
    }

    public static ServiceResponseMapper<? extends ValueLocator> buildDynamicResponseMapper(String prefix, List<MarketoField> columns)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        for (MarketoField column : columns) {
            String columName = buildColumnName(prefix, column.getName());
            MarketoField.MarketoDataType marketoDataType = column.getMarketoDataType();
            if (marketoDataType.getFormat().isPresent()) {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), columName, marketoDataType.getType(), marketoDataType.getFormat().get());
            }
            else {
                builder.add(new JacksonTopLevelValueLocator(column.getName()), columName, marketoDataType.getType());
            }
        }
        return builder.build();
    }

    public static List<String> getFieldNameFromMarketoFields(List<MarketoField> columns, String... excludedFields)
    {
        Set<String> excludeFields = Sets.newHashSet(excludedFields);
        List<String> extractedFields = new ArrayList<>();
        for (MarketoField column : columns) {
            if (excludeFields.contains(column.getName())) {
                continue;
            }
            extractedFields.add(column.getName());
        }
        return extractedFields;
    }

    public static String buildColumnName(String prefix, String columnName)
    {
        return prefix + "_" + columnName;
    }

    public static final List<DateRange> sliceRange(DateTime fromDate, DateTime toDate, int rangeSize)
    {
        List<DateRange> ranges = new ArrayList<>();
        while (fromDate.isBefore(toDate)) {
            DateTime nextToDate = fromDate.plusDays(rangeSize);
            if (nextToDate.isAfter(toDate)) {
                ranges.add(new DateRange(fromDate, toDate));
                break;
            }
            ranges.add(new DateRange(fromDate, nextToDate));
            fromDate = nextToDate.plusSeconds(1);
        }
        return ranges;
    }

    public static String getIdentityEndPoint(String accountId)
    {
        return "https://" + accountId + ".mktorest.com/identity";
    }

    public static String getEndPoint(String accountID)
    {
        return "https://" + accountID + ".mktorest.com";
    }

    public static  final class DateRange
    {
        public final DateTime fromDate;
        public final DateTime toDate;

        public DateRange(DateTime fromDate, DateTime toDate)
        {
            this.fromDate = fromDate;
            this.toDate = toDate;
        }

        @Override
        public String toString()
        {
            return "DateRange{" +
                    "fromDate=" + fromDate +
                    ", toDate=" + toDate +
                    '}';
        }
    }

    public static <T> T executeWithRetry(int maximumRetries, int initialRetryIntervalMillis, int maximumRetryIntervalMillis, AlwaysRetryRetryable<T> alwaysRetryRetryable) throws RetryExecutor.RetryGiveupException, InterruptedException
    {
        return RetryExecutor
                .retryExecutor()
                .withRetryLimit(maximumRetries)
                .withInitialRetryWait(initialRetryIntervalMillis)
                .withMaxRetryWait(maximumRetryIntervalMillis)
                .runInterruptible(alwaysRetryRetryable);
    }

    public abstract static class AlwaysRetryRetryable<T> implements  RetryExecutor.Retryable<T>
    {
        private static final Logger LOGGER = Exec.getLogger(AlwaysRetryRetryable.class);

        @Override
        public abstract T call() throws Exception;

        @Override
        public boolean isRetryableException(Exception exception)
        {
            return true;
        }

        @Override
        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryExecutor.RetryGiveupException
        {
            LOGGER.info("Retry [{}]/[{}] with retryWait [{}] on exception {}", retryCount, retryLimit, retryWait, exception.getMessage());
        }

        @Override
        public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException
        {
            LOGGER.info("Giving up execution on exception", lastException);
        }
    }
    public static <T, R> Iterable<R> flatMap(final Iterable<T> iterable, final Function<T, Iterable<R>> function)
    {
        final Iterator<T> iterator = iterable.iterator();
        return new Iterable<R>()
        {
            @Override
            public Iterator<R> iterator()
            {
                return new Iterator<R>()
                {
                    Iterator<R> currentIterator;
                    @Override
                    public boolean hasNext()
                    {
                        if (currentIterator != null && currentIterator.hasNext()) {
                            return true;
                        }
                        while (iterator.hasNext()) {
                            currentIterator = function.apply(iterator.next()).iterator();
                            if (currentIterator.hasNext()) {
                                return true;
                            }
                        }
                        return false;
                    }
                    @Override
                    public R next()
                    {
                        if (hasNext()) {
                            return currentIterator.next();
                        }
                        throw new NoSuchElementException();
                    }
                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
