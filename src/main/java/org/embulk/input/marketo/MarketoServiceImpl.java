package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by tai.khuu on 9/6/17.
 */
public class MarketoServiceImpl implements MarketoService
{
    private static final Logger LOGGER = Exec.getLogger(MarketoServiceImpl.class);

    private static final String DEFAULT_FILE_FORMAT = "csv";

    private MarketoRestClient marketoRestClient;

    public MarketoServiceImpl(MarketoRestClient marketoRestClient)
    {
        this.marketoRestClient = marketoRestClient;
    }

    @Override
    public File extractLead(Date startTime, Date endTime, List<String> extractedFields, String filterField, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond)
    {
        String exportID = marketoRestClient.createLeadBulkExtract(startTime, endTime, extractedFields, filterField);
        marketoRestClient.startLeadBulkExtract(exportID);
        try {
            marketoRestClient.waitLeadExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        InputStream leadBulkExtractResult = marketoRestClient.getLeadBulkExtractResult(exportID);
        return saveExtractedFile(exportID, leadBulkExtractResult);
    }

    private File saveExtractedFile(String exportID, InputStream leadBulkExtractResult)
    {
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
        String exportID = marketoRestClient.createActivityExtract(startTime, endTime);
        marketoRestClient.startActitvityBulkExtract(exportID);
        try {
            marketoRestClient.waitActitvityExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        InputStream activitiesBulkExtractResult = marketoRestClient.getActivitiesBulkExtractResult(exportID);
        return saveExtractedFile(exportID, activitiesBulkExtractResult);
    }

    @Override
    public Iterable<ObjectNode> getAllListLead(List<String> fieldNames)
    {
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLists();
        List<Iterable<ObjectNode>> iterables = new ArrayList<>();
        for (ObjectNode node : lists) {
            final String id = node.get("id").asText();
            iterables.add(Iterables.transform(marketoRestClient.getLeadsByList(id, fieldNames), new Function<ObjectNode, ObjectNode>()
            {
                @Nullable
                @Override
                public ObjectNode apply(@Nullable ObjectNode input)
                {
                    input.put(MarketoUtils.LIST_ID_COLUMN_NAME, id);
                    return input;
                }
            }));
        }
        return Iterables.concat(iterables);
    }

    @Override
    public Iterable<ObjectNode> getAllProgramLead(List<String> fieldNames)
    {
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getPrograms();
        List<Iterable<ObjectNode>> iterables = new ArrayList<>();
        for (ObjectNode node : lists) {
            final String id = node.get("id").asText();
            iterables.add(Iterables.transform(marketoRestClient.getLeadsByProgram(id, fieldNames), new Function<ObjectNode, ObjectNode>()
            {
                @Nullable
                @Override
                public ObjectNode apply(@Nullable ObjectNode input)
                {
                    input.put(MarketoUtils.PROGRAM_ID_COLUMN_NAME, id);
                    return input;
                }
            }));
        }
        return Iterables.concat(iterables);
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
