package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

    private static final String LIST_ID_COLUMN_NAME = "listId";

    private static final String PROGRAM_ID_COLUMN_NAME = "programId";

    private MarketoRestClient marketoRestClient;

    public MarketoServiceImpl(MarketoRestClient marketoRestClient)
    {
        this.marketoRestClient = marketoRestClient;
    }

    @Override
    public File extractLead(Date startTime, Date endTime, List<String> extractFields, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond)
    {
        String exportID = marketoRestClient.createLeadBulkExtract(startTime, endTime, extractFields);
        marketoRestClient.startLeadBulkExtract(exportID);
        try {
            marketoRestClient.waitLeadExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        InputStream leadBulkExtractResult = marketoRestClient.getLeadBulkExtractResult(exportID);
        final File tempDir = Files.createTempDir();
        TempFileSpace tempFiles = new TempFileSpace(tempDir);
        File tempFile = tempFiles.createTempFile(DEFAULT_FILE_FORMAT);
        try {
            Files.write(ByteStreams.toByteArray(leadBulkExtractResult), tempFile);
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
        String exportID = marketoRestClient.createActitvityExtract(startTime, endTime, null);
        marketoRestClient.startActitvityBulkExtract(exportID);
        try {
            marketoRestClient.waitActitvityExportJobComplete(exportID, pollingTimeIntervalSecond, bulkJobTimeoutSecond);
        }
        catch (InterruptedException e) {
            LOGGER.error("Exception when waiting for export job id: {}", exportID, e);
            throw new DataException("Error when wait for bulk extract");
        }
        InputStream activitiesBulkExtractResult = marketoRestClient.getActivitiesBulkExtractResult(exportID);
        final File tempDir = Files.createTempDir();
        TempFileSpace tempFiles = new TempFileSpace(tempDir);
        File tempFile = tempFiles.createTempFile(DEFAULT_FILE_FORMAT);
        try {
            Files.write(ByteStreams.toByteArray(activitiesBulkExtractResult), tempFile);
        }
        catch (IOException e) {
            LOGGER.error("Encounter exception when download bulk extract file, job id [{}]", exportID, e);
            throw new DataException("Can't download bulk extract file");
        }
        return tempFile;
    }

    @Override
    public Iterable<ObjectNode> getAllListLead(List<String> fieldNames)
    {
        RecordPagingIterable<ObjectNode> lists = marketoRestClient.getLists();
        List<Iterable<ObjectNode>> iterables = new ArrayList<>();
        for (ObjectNode node : lists) {
            final String id = node.get("id").asText();
            iterables.add(Iterables.transform(marketoRestClient.getLeadsByList(id, FluentIterable.from(fieldNames).filter(new Predicate<String>()
            {
                @Override
                public boolean apply(@Nullable String input)
                {
                    return !LIST_ID_COLUMN_NAME.equals(input);
                }
            }).toList()), new Function<ObjectNode, ObjectNode>()
            {
                @Nullable
                @Override
                public ObjectNode apply(@Nullable ObjectNode input)
                {
                    input.put(LIST_ID_COLUMN_NAME, id);
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
            iterables.add(Iterables.transform(marketoRestClient.getLeadsByProgram(id, FluentIterable.from(fieldNames).filter(new Predicate<String>()
            {
                @Override
                public boolean apply(@Nullable String input)
                {
                    return !PROGRAM_ID_COLUMN_NAME.equals(input);
                }
            }).toList()), new Function<ObjectNode, ObjectNode>()
            {
                @Nullable
                @Override
                public ObjectNode apply(@Nullable ObjectNode input)
                {
                    input.put(PROGRAM_ID_COLUMN_NAME, id);
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
    public List<Column> describeLead()
    {
        return marketoRestClient.describeLead();
    }

    @Override
    public List<Column> describeLeadByProgram()
    {
        List<Column> columns = marketoRestClient.describeLead();
        columns.add(new Column(columns.size(), PROGRAM_ID_COLUMN_NAME, Types.STRING));
        return columns;
    }

    @Override
    public List<Column> describeLeadByLists()
    {
        List<Column> columns = marketoRestClient.describeLead();
        columns.add(new Column(columns.size(), LIST_ID_COLUMN_NAME, Types.STRING));
        return columns;
    }
}
