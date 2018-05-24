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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadWithListInputPlugin extends MarketoBaseInputPluginDelegate<LeadWithListInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, LeadServiceResponseMapperBuilder.PluginTask
    {
    }

    public LeadWithListInputPlugin()
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        List<String> extractedFields = task.getExtractedFields();
        extractedFields.remove(MarketoUtils.LIST_ID_COLUMN_NAME);
        return FluentIterable.from(marketoService.getAllListLead(extractedFields)).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            LeadWithListServiceResponseMapper serviceResponseMapper = new LeadWithListServiceResponseMapper(task, marketoService);
            return serviceResponseMapper.buildServiceResponseMapper(task);
        }
    }

    private static class LeadWithListServiceResponseMapper extends LeadServiceResponseMapperBuilder<PluginTask>
    {

        public LeadWithListServiceResponseMapper(LeadWithListInputPlugin.PluginTask task, MarketoService marketoService)
        {
            super(task, marketoService);
        }

        @Override
        protected List<MarketoField> getLeadColumns()
        {
            List<MarketoField> leadColumns = super.getLeadColumns();
            leadColumns.add(new MarketoField(MarketoUtils.LIST_ID_COLUMN_NAME, MarketoField.MarketoDataType.STRING));
            return leadColumns;
        }
    }
}
