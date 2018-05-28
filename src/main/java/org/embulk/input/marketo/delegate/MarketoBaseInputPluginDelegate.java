package org.embulk.input.marketo.delegate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.embulk.base.restclient.DefaultServiceDataSplitter;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputTaskBase;
import org.embulk.base.restclient.ServiceDataSplitter;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.config.*;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.embulk.util.retryhelper.jetty92.DefaultJetty92ClientCreator;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public abstract class MarketoBaseInputPluginDelegate<T extends MarketoBaseInputPluginDelegate.PluginTask> implements RestClientInputPluginDelegate<T>
{
    public static final int PREVIEW_RECORD_LIMIT = 15;
    public interface PluginTask
            extends RestClientInputTaskBase, MarketoRestClient.PluginTask
    {
        @Config("schema_column_prefix")
        @ConfigDefault("\"mk\"")
        String getSchemaColumnPrefix();

        DateTime getJobStartTime();

        void setJobStartTime(DateTime dateTime);
    }

    @Override
    public ConfigDiff buildConfigDiff(T task, Schema schema, int taskCount, List<TaskReport> taskReports)
    {
        return Exec.newConfigDiff();
    }

    @Override
    public void validateInputTask(T task)
    {
        task.setJobStartTime(DateTime.now());
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
            return Exec.newTaskReport();
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
}
