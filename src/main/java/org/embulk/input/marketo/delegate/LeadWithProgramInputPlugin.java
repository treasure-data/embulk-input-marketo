package org.embulk.input.marketo.delegate;

import com.google.common.collect.FluentIterable;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;

import java.util.Iterator;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadWithProgramInputPlugin extends MarketoBaseInputPluginDelegate<LeadWithProgramInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, LeadServiceResponseMapperBuilder.PluginTask
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        List<String> fieldNames = task.getExtractedFields();
        fieldNames.remove(MarketoUtils.PROGRAM_ID_COLUMN_NAME);
        return FluentIterable.from(marketoService.getAllProgramLead(fieldNames)).
                transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            LeadWithProgramServiceResponseMapper serviceResponseMapper = new LeadWithProgramServiceResponseMapper(task, marketoService);
            return serviceResponseMapper.buildServiceResponseMapper(task);
        }
    }

    private static class LeadWithProgramServiceResponseMapper extends LeadServiceResponseMapperBuilder<PluginTask>
    {
        public LeadWithProgramServiceResponseMapper(LeadWithProgramInputPlugin.PluginTask task, MarketoService marketoService)
        {
            super(task, marketoService);
        }

        @Override
        protected List<MarketoField> getLeadColumns()
        {
            List<MarketoField> leadColumns = super.getLeadColumns();
            leadColumns.add(new MarketoField(MarketoUtils.PROGRAM_ID_COLUMN_NAME, MarketoField.MarketoDataType.STRING));
            return leadColumns;
        }
    }
}
