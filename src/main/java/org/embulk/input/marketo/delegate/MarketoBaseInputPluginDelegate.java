package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.DefaultServiceDataSplitter;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.embulk.input.marketo.MarketoInputPlugin.CONFIG_MAPPER_FACTORY;

/**
 * Created by tai.khuu on 9/18/17.
 */
public abstract class MarketoBaseInputPluginDelegate<T extends MarketoBaseInputPluginDelegate.PluginTask> implements RestClientInputPluginDelegate<T>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    public static final int PREVIEW_RECORD_LIMIT = 15;
    public static final String ID_LIST_SEPARATOR_CHAR = ",";

    public interface PluginTask
            extends RestClientInputTaskBase, MarketoRestClient.PluginTask
    {
        @Config("schema_column_prefix")
        @ConfigDefault("\"mk\"")
        String getSchemaColumnPrefix();

        @Config("incremental")
        @ConfigDefault("true")
        Boolean getIncremental();

        String getJobStartTime();

        void setJobStartTime(String dateTime);
    }

    @Override
    public ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void validateInputTask(T task)
    {
        task.setJobStartTime(OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
    }

    @Override
    public TaskReport ingestServiceData(T task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        if (Exec.isPreview()) {
            task.setBatchSize(PREVIEW_RECORD_LIMIT);
        }
        try (MarketoRestClient restClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(restClient);
            Iterator<ServiceRecord> serviceRecords = getServiceRecords(marketoService, task);
            int imported = 0;
            while (serviceRecords.hasNext() && (imported < PREVIEW_RECORD_LIMIT || !Exec.isPreview())) {
                ServiceRecord next = serviceRecords.next();
                recordImporter.importRecord(next, pageBuilder);
                imported++;
            }
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }
    }

    protected abstract Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, T task);

    @VisibleForTesting
    public MarketoRestClient createMarketoRestClient(PluginTask task)
    {
        return new MarketoRestClient(task);
    }

    @Override
    public ServiceDataSplitter<T> buildServiceDataSplitter(T task)
    {
        return new DefaultServiceDataSplitter();
    }

    protected Iterable<ObjectNode> getObjectsByIds(String[] inputIds, Function<Set<String>, Iterable<ObjectNode>> getByIdFunction)
    {
        final Set<String> ids = new HashSet<>();
        final List<String> invalidIds = new ArrayList<>();

        for (String inputId : inputIds) {
            String currentId = StringUtils.trimToNull(inputId);
            // ignore null or empty ids
            if (currentId == null) {
                continue;
            }

            // ignore or throw for NaN ids
            if (StringUtils.isNumeric(currentId)) {
                ids.add(currentId);
            }
            else {
                invalidIds.add(inputId);
            }
        }

        if (ids.isEmpty()) {
            throw new ConfigException("No valid Id specified");
        }

        if (!invalidIds.isEmpty()) {
            logger.warn("Ignore invalid Id(s): {}", invalidIds);
        }

        List<ObjectNode> actualList = Lists.newArrayList(getByIdFunction.apply(ids));
        if (actualList.isEmpty()) {
            throw new ConfigException("No valid Id found");
        }

        if (actualList.size() != ids.size()) {
            logNoneExistedIds(ids, actualList);
        }

        return actualList;
    }

    private void logNoneExistedIds(Set<String> ids, List<ObjectNode> actualList)
    {
        List<String> actualIds = actualList.parallelStream().map(n -> String.valueOf(n.get("id").asInt())).collect(Collectors.toList());
        List<String> missingIds = new ArrayList<>();
        for (String id : ids) {
            if (!actualIds.contains(id)) {
                missingIds.add(id);
            }
        }
        logger.warn("Ignore not exists Id(s): {}", missingIds);
    }
}
