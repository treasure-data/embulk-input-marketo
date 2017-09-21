package org.embulk.input.marketo.delegate;

import com.google.common.collect.FluentIterable;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;

import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadWithProgramInputPlugin extends MarketoBaseInputPluginDelegate<LeadWithProgramInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
    }

    @Override
    public TaskReport ingestServiceData(PluginTask task, RecordImporter recordImporter, int taskIndex, PageBuilder pageBuilder)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            List<String> fieldNames = MarketoUtils.getFieldNameFromSchema(pageBuilder.getSchema());

            FluentIterable<ServiceRecord> serviceRecords = FluentIterable.from(marketoService.getAllProgramLead(fieldNames)).
                    transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION);
            int imported = 0;
            for (ServiceRecord serviceRecord : serviceRecords) {
                if (imported >= PREVIEW_RECORD_LIMIT && Exec.isPreview()) {
                    break;
                }
                recordImporter.importRecord(serviceRecord, pageBuilder);
                imported++;
            }
        }
        return Exec.newTaskReport();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            return MarketoUtils.buildDynamicResponseMapper(marketoService.describeLeadByProgram());
        }
    }
}
