package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.bulk_extract.AllStringJacksonServiceRecord;
import org.embulk.input.marketo.bulk_extract.CsvRecordIterator;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.text.LineDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

public class ProgramMembersBulkExtractInputPlugin extends MarketoBaseInputPluginDelegate<ProgramMembersBulkExtractInputPlugin.PluginTask>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, CsvTokenizer.PluginTask
    {
        @Config("program_ids")
        @ConfigDefault("null")
        Optional<String> getProgramIds();

        @Config("polling_interval_second")
        @ConfigDefault("60")
        Integer getPollingIntervalSecond();

        @Config("bulk_job_timeout_second")
        @ConfigDefault("3600")
        Integer getBulkJobTimeoutSecond();

        Map<String, String> getProgramMemberFields();
        void setProgramMemberFields(Map<String, String> programMemberFields);

        List<Integer> getExtractedPrograms();
        void setExtractedPrograms(List<Integer> extractedPrograms);
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            ObjectNode result = marketoService.describeProgramMembers();
            JsonNode fields = result.get("fields");
            if (!fields.isArray()) {
                throw new DataException("[fields] isn't array node.");
            }
            Map<String, String> extractFields = new HashMap<>();
            for (JsonNode field : fields) {
                String dataType = field.get("dataType").asText();
                String name = field.get("name").asText();
                if (!extractFields.containsKey(name)) {
                    extractFields.put(name, dataType);
                }
            }
            task.setProgramMemberFields(extractFields);

            Iterable<ObjectNode> programsToRequest;
            List<Integer> programIds = new ArrayList<>();
            if (task.getProgramIds().isPresent() && StringUtils.isNotBlank(task.getProgramIds().get())) {
                final String[] idsStr = StringUtils.split(task.getProgramIds().get(), ID_LIST_SEPARATOR_CHAR);
                java.util.function.Function<Set<String>, Iterable<ObjectNode>> getListIds = marketoService::getProgramsByIds;
                programsToRequest = super.getObjectsByIds(idsStr, getListIds);
            }
            else {
                programsToRequest = marketoService.getPrograms();
            }
            Iterator<ObjectNode> iterator = programsToRequest.iterator();
            while (iterator.hasNext()) {
                ObjectNode program = iterator.next();
                int id = program.get("id").asInt();
                if (!programIds.contains(id)) {
                    programIds.add(id);
                }
            }
            if (programIds.size() <= 0) {
                throw new DataException("Cannot find any programs belong to this account.");
            }
            task.setExtractedPrograms(programIds);
        }
    }

    @Override
    public ConfigDiff buildConfigDiff(PluginTask task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public TaskReport ingestServiceData(final PluginTask task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        TaskReport taskReport = CONFIG_MAPPER_FACTORY.newTaskReport();
        if (Exec.isPreview()) {
            return MarketoUtils.importMockPreviewData(pageBuilder, PREVIEW_RECORD_LIMIT);
        }
        else {
            final MarketoRestClient restClient = createMarketoRestClient(task);
            final List<String> fieldNames = new ArrayList<>(task.getProgramMemberFields().keySet());
            final List<String> exportIDs = Collections.synchronizedList(new ArrayList<>());
            CompletableFuture[] listFutureExportIDs = task.getExtractedPrograms().stream()
                .map(programId -> CompletableFuture
                    .runAsync(() -> {
                        String exportJobID = restClient.createProgramMembersBulkExtract(fieldNames, programId);
                        exportIDs.add(exportJobID);
                        restClient.startProgramMembersBulkExtract(exportJobID);
                        int numberRecord;
                        try {
                            ObjectNode status = restClient.waitProgramMembersExportJobComplete(exportJobID, task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond());
                            numberRecord = status.get("numberOfRecords").asInt();
                        }
                        catch (InterruptedException e) {
                            logger.error("Exception when waiting for export job id: {}", exportJobID, e);
                            throw new DataException(e);
                        }
                        if (numberRecord == 0) {
                            logger.info("Export ID [{}] have no record.", exportJobID);
                            return;
                        }
                        MarketoService marketoService = new MarketoServiceImpl(restClient);
                        LineDecoder lineDecoder = null;
                        try {
                            InputStream extractedStream = new FileInputStream(marketoService.extractProgramMembers(exportJobID));
                            lineDecoder = LineDecoder.of(new InputStreamFileInput(Exec.getBufferAllocator(), extractedStream), StandardCharsets.UTF_8, null);
                            Iterator<Map<String, String>> csvRecords = new CsvRecordIterator(lineDecoder, task);
                            //Keep the preview code here when we can enable real preview
//                            if (Exec.isPreview()) {
//                                csvRecords = Iterators.limit(csvRecords, PREVIEW_RECORD_LIMIT);
//                            }
                            int imported = 0;
                            while (csvRecords.hasNext()) {
                                Map<String, String> csvRecord = csvRecords.next();
                                ObjectNode objectNode = MarketoUtils.OBJECT_MAPPER.valueToTree(csvRecord);
                                recordImporter.importRecord(new AllStringJacksonServiceRecord(objectNode), pageBuilder);
                                imported = imported + 1;
                            }

                            logger.info("Import data for export_id [{}] finish.[{}] records imported/total [{}]", exportJobID, imported, numberRecord);
                        }
                        catch (FileNotFoundException e) {
                            throw new RuntimeException("File not found", e);
                        }
                        finally {
                            if (lineDecoder != null) {
                                lineDecoder.close();
                            }
                        }
                    })
                )
                .toArray(CompletableFuture[]::new);

            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(listFutureExportIDs);
            try {
                combinedFuture.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            finally {
                restClient.close();
            }
            return taskReport;
        }
    }

    @Override
    protected final Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(ProgramMembersBulkExtractInputPlugin.PluginTask task)
    {
        List<MarketoField> programMembersColumns = new ArrayList<>();
        for (Map.Entry<String, String> entry : task.getProgramMemberFields().entrySet()) {
            programMembersColumns.add(new MarketoField(entry.getKey(), entry.getValue()));
        }
        return MarketoUtils.buildDynamicResponseMapper(task.getSchemaColumnPrefix(), programMembersColumns);
    }
}
