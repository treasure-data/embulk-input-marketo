package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.util.Fields;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoBulkExtractRequest;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.input.marketo.model.filter.DateRangeFilter;
import org.embulk.input.marketo.model.filter.MarketoFilter;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tai.khuu on 8/22/17.
 */
public class MarketoRestClient extends MarketoBaseRestClient
{
    private static final String BATCH_SIZE = "batchSize";

    private static final String NEXT_PAGE_TOKEN = "nextPageToken";

    private static final String OFFSET = "offset";

    private static final String MAX_RETURN = "maxReturn";

    private static final String MAX_BATCH_SIZE = "300";

    private static final String DEFAULT_MAX_RETURN = "200";

    private static final String RANGE_HEADER = "Range";

    private String endPoint;

    private Integer batchSize;

    private Integer maxReturn;

    private static final Logger LOGGER = Exec.getLogger(MarketoRestClient.class.getCanonicalName());

    private static final Map<String, Type> TYPE_MAPPING = new ImmutableMap.Builder<String, Type>()
            .put("datetime", Types.TIMESTAMP)
            .put("email", Types.STRING)
            .put("float", Types.DOUBLE)
            .put("integer", Types.LONG)
            .put("formula", Types.STRING)
            .put("percent", Types.LONG)
            .put("url", Types.STRING)
            .put("phone", Types.STRING)
            .put("textarea", Types.STRING)
            .put("text", Types.STRING)
            .put("string", Types.STRING)
            .put("score", Types.LONG)
            .put("boolean", Types.BOOLEAN)
            .put("currency", Types.DOUBLE)
            .put("date", Types.TIMESTAMP)
            .put("reference", Types.STRING)
            .build();

    public interface PluginTask extends Task
    {
        @Config("account_id")
        String getAccountId();

        @Config("client_secret")
        String getClientSecret();

        @Config("client_id")
        String getClientId();

        @Config("marketo_limit_interval_milis")
        @ConfigDefault("20000")
        Integer getMarketoLimitIntervalMilis();

        @Config("batch_size")
        @ConfigDefault("300")
        Integer getBatchSize();
        void setBatchSize(Integer batchSize);

        @Config("max_return")
        @ConfigDefault("200")
        Integer getMaxReturn();
        void setMaxReturn(Integer maxReturn);
    }

    public MarketoRestClient(PluginTask task, Jetty92RetryHelper retryHelper)
    {
        this(MarketoUtils.getEndPoint(task.getAccountId()), MarketoUtils.getIdentityEndPoint(task.getAccountId()), task.getClientId(), task.getClientSecret(), task.getBatchSize(), task.getMaxReturn(), task.getMarketoLimitIntervalMilis(), retryHelper);
    }

    public MarketoRestClient(String endPoint, String identityEndPoint, String clientId, String clientSecret, Integer batchSize, Integer maxReturn, int marketoLimitIntervalMilis, Jetty92RetryHelper retryHelper)
    {
        super(identityEndPoint, clientId, clientSecret, marketoLimitIntervalMilis, retryHelper);
        this.endPoint = endPoint;
        this.batchSize = batchSize;
        this.maxReturn = maxReturn;
    }

    public List<MarketoField> describeLead()
    {
        MarketoResponse<ObjectNode> jsonResponse = doGet(endPoint + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint(), null, null, new MarketoResponseJetty92EntityReader<ObjectNode>(READ_TIMEOUT_MILLIS));
        List<MarketoField> marketoFields = new ArrayList<>();
        List<ObjectNode> fields = jsonResponse.getResult();
        for (int i = 0; i < fields.size(); i++) {
            ObjectNode field = fields.get(i);
            String dataType = field.get("dataType").asText();
            if (field.has("rest")) {
                ObjectNode restField = (ObjectNode) field.get("rest");
                String name = restField.get("name").asText();
                marketoFields.add(new MarketoField(name, dataType));
            }
        }
        return marketoFields;
    }

    private Type getType(String dataType)
    {
        return TYPE_MAPPING.containsKey(dataType.toLowerCase()) ? TYPE_MAPPING.get(dataType.toLowerCase()) : Types.STRING;
    }

    public String createLeadBulkExtract(Date startTime, Date endTime, List<String> extractFields, String fitlerField)
    {
        MarketoBulkExtractRequest marketoBulkExtractRequest = getMarketoBulkExtractRequest(startTime, endTime, extractFields, fitlerField);
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_LEAD_EXTRACT);
    }

    private MarketoBulkExtractRequest getMarketoBulkExtractRequest(Date startTime, Date endTime, List<String> extractFields, String rangeFilterName)
    {
        SimpleDateFormat timeFormat = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        MarketoBulkExtractRequest marketoBulkExtractRequest = new MarketoBulkExtractRequest();
        if (extractFields != null) {
            marketoBulkExtractRequest.setFields(extractFields);
        }
        marketoBulkExtractRequest.setFormat("CSV");
        Map<String, MarketoFilter> filterMap = new HashMap<>();
        DateRangeFilter dateRangeFilter = new DateRangeFilter();
        dateRangeFilter.setStartAt(timeFormat.format(startTime));
        dateRangeFilter.setEndAt(timeFormat.format(endTime));
        filterMap.put(rangeFilterName, dateRangeFilter);
        marketoBulkExtractRequest.setFilter(filterMap);
        return marketoBulkExtractRequest;
    }

    public String createActivityExtract(Date startTime, Date endTime)
    {
        MarketoBulkExtractRequest marketoBulkExtractRequest = getMarketoBulkExtractRequest(startTime, endTime, null, "createdAt");
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT);
    }

    public String sendCreateBulkExtractRequest(MarketoBulkExtractRequest request, MarketoRESTEndpoint endpoint)
    {
        MarketoResponse<ObjectNode> marketoResponse = null;
        try {
            LOGGER.info("Send bulk extract request [{}]", request);
            marketoResponse = doPost(endPoint + endpoint.getEndpoint(), null, null, OBJECT_MAPPER.writeValueAsString(request), new MarketoResponseJetty92EntityReader<ObjectNode>(READ_TIMEOUT_MILLIS));
        }
        catch (JsonProcessingException e) {
            LOGGER.error("Encounter exception when deserialize bulk extract request", e);
            throw new DataException("Can't create bulk extract");
        }
        if (!marketoResponse.isSuccess()) {
            MarketoError marketoError = marketoResponse.getErrors().get(0);
            throw new DataException(marketoError.getCode() + ": " + marketoError.getMessage());
        }
        ObjectNode objectNode = marketoResponse.getResult().get(0);
        return objectNode.get("exportId").asText();
    }

    public void startLeadBulkExtract(String exportId)
    {
        startBulkExtract(MarketoRESTEndpoint.START_LEAD_EXPORT_JOB, exportId);
    }

    public void startActitvityBulkExtract(String exportId)
    {
        startBulkExtract(MarketoRESTEndpoint.START_ACTIVITY_EXPORT_JOB, exportId);
    }

    private void startBulkExtract(MarketoRESTEndpoint marketoRESTEndpoint, String exportId)
    {
        MarketoResponse<ObjectNode> marketoResponse = doPost(endPoint + marketoRESTEndpoint.getEndpoint(
                new ImmutableMap.Builder<String, String>().put("export_id", exportId).build()), null, null, null,
                new MarketoResponseJetty92EntityReader<ObjectNode>(READ_TIMEOUT_MILLIS));
        if (!marketoResponse.isSuccess()) {
            MarketoError error = marketoResponse.getErrors().get(0);
            throw new DataException(String.format("Can't start job for export Job id : %s, error code: %s, error message: %s", exportId, error.getCode(), error.getMessage()));
        }
    }

    /**
     * Wait for lead bulk extract job
     * Will block and wait until job status switch to complete
     * If job run logger than bulk job timeout then will stop and throw exception
     * If job status is failed or cancel will also throw exception
     *
     * @param exportId
     * @throws InterruptedException
     */
    public void waitLeadExportJobComplete(String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        waitExportJobComplete(MarketoRESTEndpoint.GET_LEAD_EXPORT_STATUS, exportId, pollingInterval, waitTimeout);
    }

    /**
     * Wait for activites bulk extract job
     * Will block and wait until job status switch to complete
     * If job run logger than bulk job timeout then will stop and throw exception
     * If job status is failed or cancel will also throw exception
     *
     * @param exportId
     * @throws InterruptedException
     */
    public void waitActitvityExportJobComplete(String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        waitExportJobComplete(MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_STATUS, exportId, pollingInterval, waitTimeout);
    }

    private void waitExportJobComplete(MarketoRESTEndpoint marketoRESTEndpoint, String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        long waitTime = 0;
        long waitTimeoutMs = waitTimeout * 1000;
        long now = System.currentTimeMillis();
        while (true) {
            MarketoResponse<ObjectNode> marketoResponse = doGet(this.endPoint + marketoRESTEndpoint.getEndpoint(
                    new ImmutableMap.Builder<String, String>().put("export_id", exportId).build()), null, null, new MarketoResponseJetty92EntityReader<ObjectNode>(READ_TIMEOUT_MILLIS));
            if (marketoResponse.isSuccess()) {
                ObjectNode objectNode = marketoResponse.getResult().get(0);
                String status = objectNode.get("status").asText();
                if (status == null) {
                    throw new DataException("Can't get bulk extract status export job id: " + exportId);
                }
                LOGGER.info("Jobs [{}] status is [{}]", exportId, status);
                switch (status) {
                    case "Completed":
                        LOGGER.info("Total wait time ms is [{}]", waitTime);
                        return;
                    case "Failed":
                        throw new DataException("Bulk extract job failed exportId: " + exportId + " errorMessage: " + objectNode.get("errorMsg").asText());
                    case "Cancel":
                        throw new DataException("Bulk extract job canceled, exportId: " + exportId);
                }
            }
            Thread.sleep(pollingInterval * 1000);
            waitTime = System.currentTimeMillis() - now;
            if (waitTime >= waitTimeoutMs) {
                throw new DataException("Job timeout exception, exportJob: " + exportId + ", run longer than " + waitTimeout + " seconds");
            }
        }
    }

    public InputStream getLeadBulkExtractResult(String exportId, BulkExtractRangeHeader bulkExtractRangeHeader)
    {
        return getBulkExtractResult(MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT, exportId, bulkExtractRangeHeader);
    }

    public InputStream getActivitiesBulkExtractResult(String exportId, BulkExtractRangeHeader bulkExtractRangeHeader)
    {
        return getBulkExtractResult(MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT, exportId, bulkExtractRangeHeader);
    }

    private InputStream getBulkExtractResult(MarketoRESTEndpoint endpoint, String exportId, BulkExtractRangeHeader bulkExtractRangeHeader)
    {
        LOGGER.info("Download bulk export job [{}]", exportId);
        Map<String, String> headers = new HashMap<>();
        if (bulkExtractRangeHeader != null) {
            headers.put(RANGE_HEADER, bulkExtractRangeHeader.toRangeHeaderValue());
            LOGGER.info("Range header value [{}]", bulkExtractRangeHeader.toRangeHeaderValue());
        }
        return doGet(this.endPoint + endpoint.getEndpoint(new ImmutableMap.Builder().put("export_id", exportId).build()), headers, null, new MarketoInputStreamResponseEntityReader(READ_TIMEOUT_MILLIS));
    }

    public RecordPagingIterable<ObjectNode> getLists()
    {
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_LISTS.getEndpoint(), new ImmutableListMultimap.Builder<String, String>().put(BATCH_SIZE, MAX_BATCH_SIZE).build(), ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getPrograms()
    {
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint(), new ImmutableListMultimap.Builder<String, String>().put(MAX_RETURN, DEFAULT_MAX_RETURN).build(), ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getLeadsByProgram(String programId, String fieldNames)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("fields", fieldNames);
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(new ImmutableMap.Builder().put("program_id", programId).build()), multimap, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getLeadsByList(String listId, String fieldNames)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("fields", fieldNames);
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(new ImmutableMap.Builder().put("list_id", listId).build()), multimap, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getCampaign()
    {
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint(), null, ObjectNode.class);
    }
    private <T> RecordPagingIterable<T> getRecordWithOffsetPagination(final String endPoint, final Multimap<String, String> parameters, final Class<T> recordClass)
    {
        return new RecordPagingIterable<>(new RecordPagingIterable.PagingFunction<RecordPagingIterable.OffsetPage<T>>()
        {
            @Override
            public RecordPagingIterable.OffsetPage<T> getNextPage(RecordPagingIterable.OffsetPage<T> currentPage)
            {
                return getOffsetPage(currentPage.getNextOffSet());
            }

            @Override
            public RecordPagingIterable.OffsetPage<T> getFirstPage()
            {
                return getOffsetPage(0);
            }

            private RecordPagingIterable.OffsetPage<T> getOffsetPage(int offset)
            {
                ImmutableListMultimap.Builder<String, String> params = new ImmutableListMultimap.Builder<>();
                params.put(OFFSET, String.valueOf(offset));
                params.put(MAX_RETURN, String.valueOf(maxReturn));
                if (parameters != null) {
                    params.putAll(parameters);
                }
                MarketoResponse<T> marketoResponse = doGet(endPoint, null, params.build(), new MarketoResponseJetty92EntityReader<>(READ_TIMEOUT_MILLIS, recordClass));
                return new RecordPagingIterable.OffsetPage<>(marketoResponse.getResult(), offset + marketoResponse.getResult().size(), marketoResponse.getResult().size() == maxReturn);
            }
        });
    }
    private <T> RecordPagingIterable<T> getRecordWithTokenPagination(final String endPoint, final Multimap<String, String> parameters, final Class<T> recordClass)
    {
        return new RecordPagingIterable<>(new RecordPagingIterable.PagingFunction<RecordPagingIterable.TokenPage<T>>()
        {
            @Override
            public RecordPagingIterable.TokenPage<T> getNextPage(RecordPagingIterable.TokenPage<T> currentPage)
            {
                return getTokenPage(currentPage);
            }
            @Override
            public RecordPagingIterable.TokenPage<T> getFirstPage()
            {
                return getTokenPage(null);
            }

            @SuppressWarnings("unchecked")
            private RecordPagingIterable.TokenPage<T> getTokenPage(RecordPagingIterable.TokenPage page)
            {
                ImmutableListMultimap.Builder params = new ImmutableListMultimap.Builder<>();
                params.put("_method", "GET");
                Fields fields = new Fields();
                if (page != null) {
                    fields.add(NEXT_PAGE_TOKEN, page.getNextPageToken());
                }
                fields.add(BATCH_SIZE, String.valueOf(batchSize));
                if (parameters != null) {
                    for (String key : parameters.keySet()) {
                        //params that is passed in should overwrite default
                        fields.remove(key);
                        for (String value : parameters.get(key)) {
                            fields.add(key, value);
                        }
                    }
                }
                //Let do GET Disguise in POST here to overcome Marketo URI Too long error
                FormContentProvider formContentProvider = new FormContentProvider(fields);
                MarketoResponse<T> marketoResponse = doPost(endPoint, null, params.build(), new MarketoResponseJetty92EntityReader<>(READ_TIMEOUT_MILLIS, recordClass), formContentProvider);
                return new RecordPagingIterable.TokenPage<>(marketoResponse.getResult(), marketoResponse.getNextPageToken(), marketoResponse.getNextPageToken() != null);
            }
        });
    }
}
