package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.embulk.input.marketo.model.BulkExtractRangeHeader;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
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

    private static final int BUF_SIZE = 0x1000;

    private static final int MAX_RESUME_TIME = 50;

    private MarketoRestClient marketoRestClient;

    public MarketoServiceImpl(MarketoRestClient marketoRestClient)
    {
        this.marketoRestClient = marketoRestClient;
    }

    @Override
    public File extractLead(final Date startTime, Date endTime, List<String> extractedFields, String filterField, int pollingTimeIntervalSecond, final int bulkJobTimeoutSecond)
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
        return downloadBulkExtract(new Function<BulkExtractRangeHeader, InputStream>()
        {
            @Override
            public InputStream apply(BulkExtractRangeHeader bulkExtractRangeHeader)
            {
                return marketoRestClient.getLeadBulkExtractResult(exportID, bulkExtractRangeHeader);
            }
        });
    }

    private long saveExtractedFile(InputStream extractResult, File tempFile) throws DownloadBulkExtractException
    {
        long total = 0;
        try (OutputStream fileOuputStream = new FileOutputStream(tempFile, true)) {
            byte[] buf = new byte[BUF_SIZE];
            while (true) {
                int r = extractResult.read(buf);
                if (r == -1) {
                    break;
                }
                fileOuputStream.write(buf, 0, r);
                total += r;
            }
        }
        catch (IOException e) {
            LOGGER.error("Encounter exception when download bulk extract file", e);
            throw new DownloadBulkExtractException("Encounter exception when download bulk extract file", e, total);
        }
        return total;
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
        return downloadBulkExtract(new Function<BulkExtractRangeHeader, InputStream>()
        {
            @Override
            public InputStream apply(BulkExtractRangeHeader bulkExtractRangeHeader)
            {
                return marketoRestClient.getActivitiesBulkExtractResult(exportID, bulkExtractRangeHeader);
            }
        });
    }
    private File downloadBulkExtract(Function<BulkExtractRangeHeader, InputStream> getBulkExtractfunction)
    {
        final File tempFile = Exec.getTempFileSpace().createTempFile(DEFAULT_FILE_FORMAT);
        long startByte = 0;
        int resumeTime = 0;
        while (resumeTime < MAX_RESUME_TIME) {
            BulkExtractRangeHeader bulkExtractRangeHeader = new BulkExtractRangeHeader(startByte);
            InputStream bulkExtractResult = getBulkExtractfunction.apply(bulkExtractRangeHeader);
            try {
                saveExtractedFile(bulkExtractResult, tempFile);
                return tempFile;
            }
            catch (DownloadBulkExtractException e) {
                startByte = startByte + e.getByteWritten();
                LOGGER.warn("will resume activity bulk extract at byte [{}]", startByte);
            }
            resumeTime = resumeTime + 1;
        }
        //Too many resume we still can't get the file
        throw new DataException("Can't down load bulk extract");
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

    private static class DownloadBulkExtractException extends Exception
    {
        private final long byteWritten;

        public DownloadBulkExtractException(String message, Throwable cause, long byteWritten)
        {
            super(message, cause);
            this.byteWritten = byteWritten;
        }

        public DownloadBulkExtractException(Throwable cause, long byteWritten)
        {
            super(cause);
            this.byteWritten = byteWritten;
        }

        public long getByteWritten()
        {
            return byteWritten;
        }
    }
}
