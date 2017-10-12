package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;

/**
 * Created by tai.khuu on 9/6/17.
 */
public class MarketoServiceImpl implements MarketoService
{
    private static final Logger LOGGER = Exec.getLogger(MarketoServiceImpl.class);

    private static final String DEFAULT_FILE_FORMAT = "csv";

    private static final int MAXIMUM_RETRIES = 3;

    private static final int INITIAL_RETRY_INTERVAL_MILLIS = 20000;

    private static final int MAXIMUM_RETRY_INTERVAL_MILLIS = 120000;

    private MarketoRestClient marketoRestClient;

    public MarketoServiceImpl(MarketoRestClient marketoRestClient)
    {
        this.marketoRestClient = marketoRestClient;
    }

    @Override
    public File extractLead(Date startTime, Date endTime, List<String> extractedFields, String filterField, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond)
    {
        final String exportID = marketoRestClient.createLeadBulkExtract(startTime, endTime, extractedFields, filterField);
        marketoRestClient.startLeadBulkExtract(exportID);
        try {
            marketoRestClient.waitLeadExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        try{
            return MarketoUtils.executeWithRetry(MAXIMUM_RETRIES, INITIAL_RETRY_INTERVAL_MILLIS, MAXIMUM_RETRY_INTERVAL_MILLIS, new MarketoUtils.AlwaysRetryRetryable<File>()
            {
                @Override
                public File call() throws Exception
                {

                    InputStream leadBulkExtractResult = marketoRestClient.getLeadBulkExtractResult(exportID);
                    return saveExtractedFile(exportID, leadBulkExtractResult);
                }
            });
        }
        catch (InterruptedException | RetryExecutor.RetryGiveupException ex) {
            LOGGER.error("Exception when download result for lead export job id: {}", exportID, ex);
            throw new DataException("Error when download result for lead bulk extract");
        }
    }

    private File saveExtractedFile(String exportID, InputStream leadBulkExtractResult)
    {
        LOGGER.info("Save bulk export file", exportID);
        File tempFile = Exec.getTempFileSpace().createTempFile(DEFAULT_FILE_FORMAT);
        try (OutputStream fileOuputStream = new FileOutputStream(tempFile)) {
            ByteStreams.copy(leadBulkExtractResult, fileOuputStream);
        }
        catch (IOException e) {
            LOGGER.error("Encounter exception when download bulk extract file, job id [{}]", exportID, e);
            throw new DataException("Can't download bulk extract file");
        }
        return tempFile;
    }

    @Override
    public File extractAllActivity(Date startTime, Date endTime, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond)
    {
        final String exportID = marketoRestClient.createActivityExtract(startTime, endTime);
        marketoRestClient.startActitvityBulkExtract(exportID);
        try {
            marketoRestClient.waitActitvityExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        try {
            return MarketoUtils.executeWithRetry(MAXIMUM_RETRIES, INITIAL_RETRY_INTERVAL_MILLIS, MAXIMUM_RETRY_INTERVAL_MILLIS, new MarketoUtils.AlwaysRetryRetryable<File>()
            {
                @Override
                public File call() throws Exception
                {
                    InputStream activitiesBulkExtractResult = marketoRestClient.getActivitiesBulkExtractResult(exportID);
                    return saveExtractedFile(exportID, activitiesBulkExtractResult);
                }
            });
        }
        catch (InterruptedException | RetryExecutor.RetryGiveupException ex) {
            LOGGER.error("Exception when download result for activity export job id: {}", exportID, ex);
            throw new DataException("Error when download result for activity bulk extract");
        }
    }

    @Override
    public Iterable<ObjectNode> getAllListLead(List<String> fieldNames)
    {
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLists();
        final String fieldNameString = StringUtils.join(fieldNames, ",");
        return MarketoUtils.flatMap(lists, new Function<ObjectNode, Iterable<ObjectNode>>()
        {
            @Override
            public Iterable<ObjectNode> apply(ObjectNode input)
            {
                final String id = input.get("id").asText();
                return Iterables.transform(marketoRestClient.getLeadsByList(id, fieldNameString), new Function<ObjectNode, ObjectNode>()
                {
                    @Override
                    public ObjectNode apply(ObjectNode input)
                    {
                        input.put(MarketoUtils.LIST_ID_COLUMN_NAME, id);
                        return input;
                    }
                });
            }
        });
    }

    @Override
    public Iterable<ObjectNode> getAllProgramLead(List<String> fieldNames)
    {
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getPrograms();
        final String fieldNameString = StringUtils.join(fieldNames, ",");
        return MarketoUtils.flatMap(lists, new Function<ObjectNode, Iterable<ObjectNode>>()
        {
            @Override
            public Iterable<ObjectNode> apply(ObjectNode input)
            {
                final String id = input.get("id").asText();
                return Iterables.transform(marketoRestClient.getLeadsByProgram(id, fieldNameString), new Function<ObjectNode, ObjectNode>()
                {
                    @Override
                    public ObjectNode apply(ObjectNode input)
                    {
                        input.put(MarketoUtils.PROGRAM_ID_COLUMN_NAME, id);
                        return input;
                    }
                });
            }
        });
    }

    @Override
    public RecordPagingIterable<ObjectNode> getCampaign()
    {
        return marketoRestClient.getCampaign();
    }

    @Override
    public List<MarketoField> describeLead()
    {
        return marketoRestClient.describeLead();
    }

    @Override
    public List<MarketoField> describeLeadByProgram()
    {
        List<MarketoField> columns = marketoRestClient.describeLead();
        columns.add(new MarketoField(MarketoUtils.PROGRAM_ID_COLUMN_NAME, MarketoField.MarketoDataType.STRING));
        return columns;
    }

    @Override
    public List<MarketoField> describeLeadByLists()
    {
        List<MarketoField> columns = marketoRestClient.describeLead();
        columns.add(new MarketoField(MarketoUtils.LIST_ID_COLUMN_NAME, MarketoField.MarketoDataType.STRING));
        return columns;
    }
}
