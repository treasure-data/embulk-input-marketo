package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.util.Fields;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoBulkExtractRequest;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.input.marketo.model.filter.DateRangeFilter;
import org.embulk.spi.DataException;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    private static final String FILTER_TYPE = "filterType";

    private static final String FILTER_VALUES = "filterValues";

    private static final String FIELDS = "fields";

    private static final int MAX_REQUEST_SIZE = 300;

    private static final int CONNECT_TIMEOUT_IN_MILLIS = 30000;
    private static final int IDLE_TIMEOUT_IN_MILLIS = 60000;

    private final String endPoint;

    private final Integer batchSize;

    private final Integer maxReturn;

    private final Logger logger = LoggerFactory.getLogger(getClass());

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

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getInputEndpoint();

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

        @Config("read_timeout_millis")
        @ConfigDefault("60000")
        Long getReadTimeoutMillis();

        @Config("maximum_retries")
        @ConfigDefault("7")
        Integer getMaximumRetries();

        @Config("initial_retry_interval_milis")
        @ConfigDefault("20000")
        Integer getInitialRetryIntervalMilis();

        @Config("maximum_retries_interval_milis")
        @ConfigDefault("120000")
        Integer getMaximumRetriesIntervalMilis();

        @Config("partner_api_key")
        @ConfigDefault("null")
        Optional<String> getPartnerApiKey();
    }

    public MarketoRestClient(PluginTask task)
    {
        this(MarketoUtils.getEndPoint(task.getAccountId(), task.getInputEndpoint()),
                MarketoUtils.getIdentityEndPoint(task.getAccountId(), task.getInputEndpoint()),
                task.getClientId(),
                task.getClientSecret(),
                task.getAccountId(),
                task.getPartnerApiKey(),
                task.getBatchSize(),
                task.getMaxReturn(),
                task.getReadTimeoutMillis(),
                task.getMarketoLimitIntervalMilis(),
                new Jetty92RetryHelper(task.getMaximumRetries(),
                        task.getInitialRetryIntervalMilis(),
                        task.getMaximumRetriesIntervalMilis(),
                        new DefaultJetty92ClientCreator(CONNECT_TIMEOUT_IN_MILLIS, IDLE_TIMEOUT_IN_MILLIS)));
    }

    public MarketoRestClient(String endPoint,
                             String identityEndPoint,
                             String clientId,
                             String clientSecret,
                             String accountId,
                             Optional<String> partnerApiKey,
                             Integer batchSize,
                             Integer maxReturn,
                             long readTimeoutMilis,
                             int marketoLimitIntervalMilis,
                             Jetty92RetryHelper retryHelper)
    {
        super(identityEndPoint, clientId, clientSecret, accountId, partnerApiKey, marketoLimitIntervalMilis, readTimeoutMilis, retryHelper);
        this.endPoint = endPoint;
        this.batchSize = batchSize;
        this.maxReturn = maxReturn;
    }

    public List<MarketoField> describeLead()
    {
        MarketoResponse<ObjectNode> jsonResponse = doGet(endPoint + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint(), null, null, new MarketoResponseJetty92EntityReader<>(this.readTimeoutMillis));
        List<MarketoField> marketoFields = new ArrayList<>();
        List<ObjectNode> fields = jsonResponse.getResult();
        for (ObjectNode field : fields) {
            String dataType = field.get("dataType").asText();
            if (field.has("rest")) {
                ObjectNode restField = (ObjectNode) field.get("rest");
                String name = restField.get("name").asText();
                marketoFields.add(new MarketoField(name, dataType));
            }
        }
        return marketoFields;
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
        Map<String, Object> filterMap = new HashMap<>();
        DateRangeFilter dateRangeFilter = new DateRangeFilter();
        dateRangeFilter.setStartAt(timeFormat.format(startTime));
        dateRangeFilter.setEndAt(timeFormat.format(endTime));
        filterMap.put(rangeFilterName, dateRangeFilter);
        marketoBulkExtractRequest.setFilter(filterMap);
        return marketoBulkExtractRequest;
    }

    public String createActivityExtract(List<Integer> activityTypeIds, Date startTime, Date endTime)
    {
        MarketoBulkExtractRequest marketoBulkExtractRequest = getMarketoBulkExtractRequest(startTime, endTime, null, "createdAt");
        if (activityTypeIds != null && !activityTypeIds.isEmpty()) {
            marketoBulkExtractRequest.getFilter().put("activityTypeIds", activityTypeIds);
        }
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT);
    }

    public String sendCreateBulkExtractRequest(MarketoBulkExtractRequest request, MarketoRESTEndpoint endpoint)
    {
        MarketoResponse<ObjectNode> marketoResponse;
        try {
            logger.info("Send bulk extract request [{}]", request);
            marketoResponse = doPost(endPoint + endpoint.getEndpoint(), null, null, OBJECT_MAPPER.writeValueAsString(request), new MarketoResponseJetty92EntityReader<>(readTimeoutMillis));
        }
        catch (JsonProcessingException e) {
            logger.error("Encounter exception when deserialize bulk extract request", e);
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
                new MarketoResponseJetty92EntityReader<>(readTimeoutMillis));
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
     */
    public void waitActitvityExportJobComplete(String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        waitExportJobComplete(MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_STATUS, exportId, pollingInterval, waitTimeout);
    }

    private ObjectNode waitExportJobComplete(MarketoRESTEndpoint marketoRESTEndpoint, String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        long waitTime = 0;
        long waitTimeoutMs = waitTimeout * 1000L;
        long now = System.currentTimeMillis();
        while (true) {
            MarketoResponse<ObjectNode> marketoResponse = doGet(this.endPoint + marketoRESTEndpoint.getEndpoint(
                    new ImmutableMap.Builder<String, String>().put("export_id", exportId).build()), null, null, new MarketoResponseJetty92EntityReader<>(readTimeoutMillis));
            if (marketoResponse.isSuccess()) {
                ObjectNode objectNode = marketoResponse.getResult().get(0);
                String status = objectNode.get("status").asText();
                if (status == null) {
                    throw new DataException("Can't get bulk extract status export job id: " + exportId);
                }
                logger.info("Jobs [{}] status is [{}]", exportId, status);
                switch (status) {
                    case "Completed":
                        logger.info("Total wait time ms is [{}]", waitTime);
                        logger.info("File size is [{}] bytes", objectNode.get("fileSize"));
                        return objectNode;
                    case "Failed":
                        throw new DataException("Bulk extract job failed exportId: " + exportId + " errorMessage: " + objectNode.get("errorMsg").asText());
                    case "Cancel":
                        throw new DataException("Bulk extract job canceled, exportId: " + exportId);
                }
            }
            Thread.sleep(pollingInterval * 1000L);
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
        logger.info("Download bulk export job [{}]", exportId);
        Map<String, String> headers = new HashMap<>();
        if (bulkExtractRangeHeader != null) {
            headers.put(RANGE_HEADER, bulkExtractRangeHeader.toRangeHeaderValue());
            logger.info("Range header value [{}]", bulkExtractRangeHeader.toRangeHeaderValue());
        }
        return doGet(this.endPoint + endpoint.getEndpoint(new ImmutableMap.Builder().put("export_id", exportId).build()), headers, null, new MarketoInputStreamResponseEntityReader(readTimeoutMillis));
    }

    public RecordPagingIterable<ObjectNode> getLists()
    {
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_LISTS.getEndpoint(), new ImmutableListMultimap.Builder<String, String>().put(BATCH_SIZE, MAX_BATCH_SIZE).build(), ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getListsByIds(Set<String> ids)
    {
        Multimap<String, String> params = new ImmutableListMultimap
                .Builder<String, String>()
                .put("id", StringUtils.join(ids, ","))
                .put(BATCH_SIZE, MAX_BATCH_SIZE).build();
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_LISTS.getEndpoint(), params, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getPrograms()
    {
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint(), new ImmutableListMultimap.Builder<String, String>().put(MAX_RETURN, DEFAULT_MAX_RETURN).build(), ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getProgramsByIds(Set<String> ids)
    {
        Multimap<String, String> params = new ImmutableListMultimap
                .Builder<String, String>()
                .put(FILTER_TYPE, "id")
                .put(FILTER_VALUES, StringUtils.join(ids, ","))
                .put(BATCH_SIZE, MAX_BATCH_SIZE).build();
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint(), params, ObjectNode.class);
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
                MarketoResponse<T> marketoResponse = doGet(endPoint, null, params.build(), new MarketoResponseJetty92EntityReader<>(readTimeoutMillis, recordClass));
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
                MarketoResponse<T> marketoResponse = doPost(endPoint, null, params.build(), new MarketoResponseJetty92EntityReader<>(readTimeoutMillis, recordClass), formContentProvider);
                return new RecordPagingIterable.TokenPage<>(marketoResponse.getResult(), marketoResponse.getNextPageToken(), marketoResponse.getNextPageToken() != null);
            }
        });
    }

    public Iterable<ObjectNode> getProgramsByTag(String tagType, String tagValue)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("tagType", tagType);
        multimap.put("tagValue", tagValue);
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS_BY_TAG.getEndpoint(), multimap, ObjectNode.class);
    }

    public Iterable<ObjectNode> getProgramsByDateRange(Date earliestUpdatedAt, Date latestUpdatedAt, String filterType, List<String> filterValues)
    {
        SimpleDateFormat timeFormat = new SimpleDateFormat(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("earliestUpdatedAt", timeFormat.format(earliestUpdatedAt));
        multimap.put("latestUpdatedAt", timeFormat.format(latestUpdatedAt));
        // put filter params if exist.
        if (filterType != null) {
            multimap.put("filterType", filterType);
            multimap.put("filterValues", StringUtils.join(filterValues, ","));
        }
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint(), multimap, ObjectNode.class);
    }

    public List<MarketoField> describeCustomObject(String apiName)
    {
        MarketoResponse<ObjectNode> jsonResponse = doGet(endPoint + MarketoRESTEndpoint.GET_CUSTOM_OBJECT_DESCRIBE.getEndpoint(new ImmutableMap.Builder().put("api_name", apiName).build()), null, null, new MarketoResponseJetty92EntityReader<>(this.readTimeoutMillis));
        if (jsonResponse.getResult().size() == 0) {
            throw new ConfigException(String.format("Custom Object %s is not exits.", apiName));
        }
        List<MarketoField> marketoFields = new ArrayList<>();
        JsonNode fieldNodes = jsonResponse.getResult().get(0).path("fields");
        for (JsonNode node : fieldNodes) {
            String dataType = node.get("dataType").asText();
            String name = node.get("name").asText();
            marketoFields.add(new MarketoField(name, dataType));
        }
        if (marketoFields.size() == 0) {
            throw new ConfigException(String.format("Custom Object %s don't have any field data.", apiName));
        }
        return marketoFields;
    }
    private <T> RecordPagingIterable<T> getCustomObjectRecordWithPagination(final String endPoint, final String customObjectFilterType, final String customObjectFields, final Integer fromValue, final Integer toValue, final Class<T> recordClass)
    {
        return new RecordPagingIterable<>(new RecordPagingIterable.PagingFunction<RecordPagingIterable.OffsetWithTokenPage<T>>()
        {
            @Override
            public RecordPagingIterable.OffsetWithTokenPage<T> getNextPage(RecordPagingIterable.OffsetWithTokenPage<T> currentPage)
            {
                return getOffsetPage(currentPage.getNextOffSet(), currentPage.getNextPageToken());
            }

            @Override
            public RecordPagingIterable.OffsetWithTokenPage<T> getFirstPage()
            {
                return getOffsetPage(fromValue, "");
            }

            private RecordPagingIterable.OffsetWithTokenPage<T> getOffsetPage(int offset, String nextPageToken)
            {
                boolean isMoreResult = true;
                boolean isEndOffset = false;
                int nextOffset = offset + MAX_REQUEST_SIZE;

                if (toValue != null) {
                    if (toValue <= nextOffset) {
                        nextOffset = toValue + 1;
                        isEndOffset = true;
                    }
                }
                StringBuilder filterValues = new StringBuilder();
                for (int i = offset; i < (nextOffset - 1); i++) {
                    filterValues.append(String.valueOf(i)).append(",");
                }
                filterValues.append(String.valueOf(nextOffset - 1));

                ImmutableListMultimap.Builder<String, String> params = new ImmutableListMultimap.Builder<>();
                params.put(FILTER_TYPE, customObjectFilterType);
                params.put(FILTER_VALUES, filterValues.toString());
                if (StringUtils.isNotBlank(nextPageToken)) {
                    params.put(NEXT_PAGE_TOKEN, nextPageToken);
                }
                if (customObjectFields != null) {
                    params.put(FIELDS, customObjectFields);
                }
                MarketoResponse<T> marketoResponse = doGet(endPoint, null, params.build(), new MarketoResponseJetty92EntityReader<>(readTimeoutMillis, recordClass));
                String nextToken = "";
                if (StringUtils.isNotBlank(marketoResponse.getNextPageToken())) {
                    nextToken = marketoResponse.getNextPageToken();
                    //skip offset when nextPageToken is exits.
                    nextOffset = offset;
                }

                if (toValue == null) {
                    if (marketoResponse.getResult().isEmpty()) {
                        isMoreResult = false;
                    }
                }
                else {
                    if (isEndOffset && StringUtils.isBlank(nextToken)) {
                        isMoreResult = false;
                    }
                }
                return new RecordPagingIterable.OffsetWithTokenPage<>(marketoResponse.getResult(), nextOffset, nextToken, isMoreResult);
            }
        });
    }

    public Iterable<ObjectNode> getCustomObject(String customObjectApiName, String filterType, String filterValue, String returnFields)
    {
        Multimap<String, String> params = new ImmutableListMultimap
                .Builder<String, String>()
                .put("filterType", StringUtils.trimToEmpty(filterType))
                .put("filterValues", StringUtils.trimToEmpty(filterValue))
                .put("fields", StringUtils.trimToEmpty(returnFields))
                .put(BATCH_SIZE, MAX_BATCH_SIZE).build();
        return getRecordWithTokenPagination(endPoint + MarketoRESTEndpoint.GET_CUSTOM_OBJECT.getEndpoint(new ImmutableMap.Builder().put("api_name", customObjectApiName).build()), params, ObjectNode.class);
    }

    public Iterable<ObjectNode> getCustomObject(String customObjectAPIName, String customObjectFilterType, String customObjectFields, Integer fromValue, Integer toValue)
    {
        return getCustomObjectRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_CUSTOM_OBJECT.getEndpoint(new ImmutableMap.Builder().put("api_name", customObjectAPIName).build()), customObjectFilterType, customObjectFields, fromValue, toValue, ObjectNode.class);
    }

    public Iterable<ObjectNode> getActivityTypes()
    {
        return getRecordWithOffsetPagination(endPoint + MarketoRESTEndpoint.GET_ACTIVITY_TYPES.getEndpoint(), new ImmutableListMultimap.Builder<String, String>().put(MAX_RETURN, DEFAULT_MAX_RETURN).build(), ObjectNode.class);
    }

    public ObjectNode describeProgramMembers()
    {
        MarketoResponse<ObjectNode> jsonResponse = doGet(endPoint + MarketoRESTEndpoint.DESCRIBE_PROGRAM_MEMBERS.getEndpoint(), null, null, new MarketoResponseJetty92EntityReader<>(this.readTimeoutMillis));
        return jsonResponse.getResult().get(0);
    }

    public String createProgramMembersBulkExtract(List<String> extractFields, int programId)
    {
        MarketoBulkExtractRequest marketoBulkExtractRequest = new MarketoBulkExtractRequest();
        if (extractFields != null) {
            marketoBulkExtractRequest.setFields(extractFields);
        }
        marketoBulkExtractRequest.setFormat("CSV");
        Map<String, Object> filterMap = new HashMap<>();
        filterMap.put("programId", programId);
        marketoBulkExtractRequest.setFilter(filterMap);
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_PROGRAM_MEMBERS_EXPORT_JOB);
    }

    public void startProgramMembersBulkExtract(String exportId)
    {
        startBulkExtract(MarketoRESTEndpoint.START_PROGRAM_MEMBERS_EXPORT_JOB, exportId);
    }

    public ObjectNode waitProgramMembersExportJobComplete(String exportId, int pollingInterval, int waitTimeout) throws InterruptedException
    {
        return waitExportJobComplete(MarketoRESTEndpoint.GET_PROGRAM_MEMBERS_EXPORT_STATUS, exportId, pollingInterval, waitTimeout);
    }

    public InputStream getProgramMemberBulkExtractResult(String exportId, BulkExtractRangeHeader bulkExtractRangeHeader)
    {
        return getBulkExtractResult(MarketoRESTEndpoint.GET_PROGRAM_MEMBERS_EXPORT_RESULT, exportId, bulkExtractRangeHeader);
    }
}
