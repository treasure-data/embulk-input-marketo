package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.input.marketo.model.MarketoBulkExtractRequest;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.input.marketo.model.filter.DateRangeFilter;
import org.embulk.input.marketo.model.filter.ListFilter;
import org.embulk.input.marketo.model.filter.MarketoFilter;
import org.embulk.spi.Column;
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
    private static final int IDLE_TIMEOUT = 5000;

    private static final int CONNECT_TIMEOUT = 5000;

    private static final String BATCH_SIZE = "batchSize";

    private static final String NEXT_PAGE_TOKEN = "nextPageToken";

    private String endPoint;

    private Integer batchSize;

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
        @Config("endpoint")
        String getEndpoint();

        @Config("identity_endpoint")
        String getIdentityEndpoint();

        @Config("client_secret")
        String getClientSecret();

        @Config("client_id")
        String getClientId();

        @Config("batch_size")
        @ConfigDefault("300")
        Integer getBatchSize();
    }

    public MarketoRestClient(PluginTask task, Jetty92RetryHelper retryHelper)
    {
        this(task.getEndpoint(), task.getIdentityEndpoint(), task.getClientId(), task.getClientSecret(), task.getBatchSize(), retryHelper);
    }

    public MarketoRestClient(String endPoint, String identityEndPoint, String clientId, String clientSecret, Integer batchSize, Jetty92RetryHelper retryHelper)
    {
        super(identityEndPoint, clientId, clientSecret, retryHelper);
        this.endPoint = endPoint;
        this.batchSize = batchSize;
    }

    public List<Column> describeLead()
    {
        MarketoResponse<ObjectNode> jsonResponse = doGet(endPoint + MarketoRESTEndpoint.DESCRIBE_LEAD.getEndpoint(), null, null, new MarketoResponseJetty92EntityReader<ObjectNode>(READ_TIMEOUT_MILLIS));
        List<Column> columnsList = new ArrayList<>();
        List<ObjectNode> fields = jsonResponse.getResult();
        for (int i = 0; i < fields.size(); i++) {
            ObjectNode field = fields.get(i);
            String dataType = field.get("dataType").asText();
            ObjectNode restField = (ObjectNode) field.get("rest");
            String name = restField.get("name").asText();
            columnsList.add(new Column(i, name, getType(dataType)));
        }
        return columnsList;
    }

    private Type getType(String dataType)
    {
        return TYPE_MAPPING.containsKey(dataType.toLowerCase()) ? TYPE_MAPPING.get(dataType.toLowerCase()) : Types.STRING;
    }

    public String createLeadBulkExtract(Date startTime, Date endTime, List<String> extractFields)
    {
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        MarketoBulkExtractRequest marketoBulkExtractRequest = new MarketoBulkExtractRequest();
        marketoBulkExtractRequest.setFields(extractFields);
        marketoBulkExtractRequest.setFormat("CSV");
        Map<String, MarketoFilter> filterMap = new HashMap<>();
        DateRangeFilter dateRangeFilter = new DateRangeFilter();
        dateRangeFilter.setStartAt(timeFormat.format(startTime));
        dateRangeFilter.setEndAt(timeFormat.format(endTime));
        filterMap.put("createdAt", dateRangeFilter);
        marketoBulkExtractRequest.setFilter(filterMap);
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_LEAD_EXTRACT);
    }

    public String createActitvityExtract(Date startTime, Date endTime, List<String> activityTypes)
    {
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        MarketoBulkExtractRequest marketoBulkExtractRequest = new MarketoBulkExtractRequest();
        marketoBulkExtractRequest.setFormat("CSV");
        Map<String, MarketoFilter> filterMap = new HashMap<>();
        DateRangeFilter dateRangeFilter = new DateRangeFilter();
        dateRangeFilter.setStartAt(timeFormat.format(startTime));
        dateRangeFilter.setEndAt(timeFormat.format(endTime));
        filterMap.put("createdAt", dateRangeFilter);
        if (activityTypes != null) {
            ListFilter activitiesTypeFilter = new ListFilter();
            activitiesTypeFilter.addAll(activityTypes);
            filterMap.put("activities", activitiesTypeFilter);
        }
        marketoBulkExtractRequest.setFilter(filterMap);
        return sendCreateBulkExtractRequest(marketoBulkExtractRequest, MarketoRESTEndpoint.CREATE_ACTIVITY_EXTRACT);
    }

    public String sendCreateBulkExtractRequest(MarketoBulkExtractRequest request, MarketoRESTEndpoint endpoint)
    {
        MarketoResponse<ObjectNode> marketoResponse = null;
        try {
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
                        return;
                    case "Failed":
                        throw new DataException("Bulk extract job failed exportId: " + exportId + " errorMessage: " + objectNode.get("errorMsg").asText());
                    case "Cancel":
                        throw new DataException("Bulk extract job canceled, exportId: " + exportId);
                }
            }
            Thread.sleep(pollingInterval * 1000);
            waitTime = waitTime + pollingInterval;
            if (waitTime >= waitTimeout) {
                throw new DataException("Job timeout exception, exportJob: " + exportId + ", run longer than " + waitTimeout + " seconds");
            }
        }
    }

    public InputStream getLeadBulkExtractResult(String exportId)
    {
        return getBulkExtractResult(MarketoRESTEndpoint.GET_LEAD_EXPORT_RESULT, exportId);
    }

    public InputStream getActivitiesBulkExtractResult(String exportId)
    {
        return getBulkExtractResult(MarketoRESTEndpoint.GET_ACTIVITY_EXPORT_RESULT, exportId);
    }

    private InputStream getBulkExtractResult(MarketoRESTEndpoint endpoint, String exportId)
    {
        InputStream fileStream = doGet(this.endPoint + endpoint.getEndpoint(new ImmutableMap.Builder().put("export_id", exportId).build()), null, null, new MarketoFileResponseEntityReader(READ_TIMEOUT_MILLIS));
        return fileStream;
    }

    public RecordPagingIterable<ObjectNode> getLists()
    {
        return getRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_LISTS.getEndpoint(), null, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getPrograms()
    {
        return getRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_PROGRAMS.getEndpoint(), null, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getLeadsByProgram(String programId, List<String> fieldNames)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("fields", StringUtils.join(fieldNames, ","));
        return getRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_LEADS_BY_PROGRAM.getEndpoint(new ImmutableMap.Builder().put("program_id", programId).build()), multimap, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getLeadsByList(String listId, List<String> fieldNames)
    {
        Multimap<String, String> multimap = ArrayListMultimap.create();
        multimap.put("fields", StringUtils.join(fieldNames, ","));
        return getRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_LEADS_BY_LIST.getEndpoint(new ImmutableMap.Builder().put("list_id", listId).build()), multimap, ObjectNode.class);
    }

    public RecordPagingIterable<ObjectNode> getCampaign()
    {
        return getRecordWithPagination(endPoint + MarketoRESTEndpoint.GET_CAMPAIGN.getEndpoint(), null, ObjectNode.class);
    }

    private <T> RecordPagingIterable<T> getRecordWithPagination(final String endPoint, final Multimap<String, String> parameters, final Class<T> recordClass)
    {
        return new RecordPagingIterable<>(new RecordPagingIterable.PagingFunction<RecordPagingIterable.MarketoPage<T>>()
        {
            @Override
            public RecordPagingIterable.MarketoPage<T> getNextPage(RecordPagingIterable.MarketoPage<T> currentPage)
            {
                final Multimap<String, String> params = ArrayListMultimap.create();
                params.put(BATCH_SIZE, batchSize + "");
                params.put(NEXT_PAGE_TOKEN, currentPage.getNextPageToken());
                if (parameters != null) {
                    params.putAll(parameters);
                }
                MarketoResponse<T> marketoResponse = doGet(endPoint, null, params, new MarketoResponseJetty92EntityReader<>(READ_TIMEOUT_MILLIS, recordClass));
                return new RecordPagingIterable.MarketoPage<>(marketoResponse.getResult(), marketoResponse.getNextPageToken(), marketoResponse.isMoreResult());
            }

            @Override
            public RecordPagingIterable.MarketoPage<T> getFirstPage()
            {
                final Multimap<String, String> params = ArrayListMultimap.create();
                params.put(BATCH_SIZE, batchSize + "");
                if (parameters != null) {
                    params.putAll(parameters);
                }
                MarketoResponse<T> marketoResponse = doGet(endPoint, null, params, new MarketoResponseJetty92EntityReader<>(READ_TIMEOUT_MILLIS, recordClass));
                return new RecordPagingIterable.MarketoPage<>(marketoResponse.getResult(), marketoResponse.getNextPageToken(), marketoResponse.isMoreResult());
            }
        });
    }
}
