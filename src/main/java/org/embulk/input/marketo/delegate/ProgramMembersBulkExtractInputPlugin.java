package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

public class ProgramMembersBulkExtractInputPlugin extends MarketoBaseInputPluginDelegate<ProgramMembersBulkExtractInputPlugin.PluginTask>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Object pageBuilderLock = new Object();

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

        //https://developers.marketo.com/rest-api/bulk-extract/
        @Max(2)
        @Min(1)
        @Config("number_concurrent_export_job")
        @ConfigDefault("2")
        Integer getNumberConcurrentExportJob();

        @Config("program_member_fields")
        @ConfigDefault("null")
        Optional<Map<String, String>> getProgramMemberFields();
        void setProgramMemberFields(Optional<Map<String, String>> programMemberFields);

        @Config("extracted_program_ids")
        @ConfigDefault("null")
        Optional<List<Integer>> getExtractedProgramIds();
        void setExtractedProgramIds(Optional<List<Integer>> extractedProgramIds);
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
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
                throw new DataException("No program belong to this account.");
            }
            task.setExtractedProgramIds(Optional.of(programIds));

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
            task.setProgramMemberFields(Optional.of(extractFields));
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
            if (!task.getProgramMemberFields().isPresent() || !task.getExtractedProgramIds().isPresent()) {
                throw new ConfigException("program_member_fields or extracted_programs are missing.");
            }
            final ThreadPoolExecutor executor = createExecutor(task);
            final MarketoRestClient restClient = createMarketoRestClient(task);

            try {
                final List<String> fieldNames = new ArrayList<>(task.getProgramMemberFields().get().keySet());

                List<Future> listFutureExportIDs = task.getExtractedProgramIds().get().stream()
                                                       .map(programId -> createFutureTask(task, recordImporter, pageBuilder, executor, restClient, fieldNames, programId))
                                                       .collect(Collectors.toList());

                for (Future future : listFutureExportIDs) {
                    try {
                        future.get();
                    }
                    catch (InterruptedException | ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            finally {
                restClient.close();
                if (!executor.isShutdown()) {
                    executor.shutdownNow();
                }
            }
            return taskReport;
        }
    }

    @VisibleForTesting
    protected ThreadPoolExecutor createExecutor(PluginTask task)
    {
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(task.getNumberConcurrentExportJob());
    }

    private Future<?> createFutureTask(PluginTask task, RecordImporter recordImporter, PageBuilder pageBuilder, ThreadPoolExecutor executor, MarketoRestClient restClient, List<String> fieldNames, Integer programId)
    {
        Runnable exportTask = () -> {
            String exportJobID = restClient.createProgramMembersBulkExtract(fieldNames, programId);
            restClient.startProgramMembersBulkExtract(exportJobID);
            int numberRecord;
            try {
                ObjectNode status = restClient.waitProgramMembersExportJobComplete(exportJobID, task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond());
                numberRecord = status.get("numberOfRecords").asInt();
            }
            catch (InterruptedException e) {
                logger.error("Exception when waiting for export program [{}], job id [{}]", programId, exportJobID, e);
                throw new DataException(e);
            }
            if (numberRecord == 0) {
                logger.info("Export program [{}], job [{}] have no record.", programId, exportJobID);
                return;
            }
            MarketoService marketoService = new MarketoServiceImpl(restClient);
            LineDecoder lineDecoder = null;
            try {
                InputStream extractedStream = new FileInputStream(marketoService.extractProgramMembers(exportJobID));
                lineDecoder = LineDecoder.of(new InputStreamFileInput(Exec.getBufferAllocator(), extractedStream), StandardCharsets.UTF_8, null);
                Iterator<Map<String, String>> csvRecords = new CsvRecordIterator(lineDecoder, task);
                int imported = 0;
                while (csvRecords.hasNext()) {
                    Map<String, String> csvRecord = csvRecords.next();
                    ObjectNode objectNode = MarketoUtils.OBJECT_MAPPER.valueToTree(csvRecord);
                    // MEMO: pageBuilderがスレッドアンセーフなために排他制御を利用する
                    synchronized (pageBuilderLock) {
                        recordImporter.importRecord(new AllStringJacksonServiceRecord(objectNode), pageBuilder);
                    }
                    imported = imported + 1;
                }

                logger.info("Import data for program [{}], job_id [{}] finish.[{}] records imported/total [{}]", programId, exportJobID, imported, numberRecord);
            }
            catch (FileNotFoundException e) {
                throw new RuntimeException("File export cannot be found", e);
            }
            finally {
                if (lineDecoder != null) {
                    lineDecoder.close();
                }
            }
        };

        Thread exportThread = new Thread(exportTask);
        exportThread.setDaemon(true);

        return executor.submit(exportThread);
    }

    @Override
    protected final Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(ProgramMembersBulkExtractInputPlugin.PluginTask task)
    {
        if (!task.getProgramMemberFields().isPresent() || task.getProgramMemberFields().get().size() <= 0) {
            throw new ConfigException("program_member_fields are missing.");
        }
        List<MarketoField> programMembersColumns = new ArrayList<>();
        for (Map.Entry<String, String> entry : task.getProgramMemberFields().get().entrySet()) {
            programMembersColumns.add(new MarketoField(entry.getKey(), entry.getValue()));
        }
        return MarketoUtils.buildDynamicResponseMapper(task.getSchemaColumnPrefix(), programMembersColumns);
    }
}
