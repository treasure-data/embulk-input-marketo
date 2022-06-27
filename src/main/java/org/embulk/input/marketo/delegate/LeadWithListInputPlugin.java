package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.FluentIterable;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadWithListInputPlugin extends MarketoBaseInputPluginDelegate<LeadWithListInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, LeadServiceResponseMapperBuilder.PluginTask
    {
        @Config("list_ids")
        @ConfigDefault("null")
        Optional<String> getListIds();
    }

    public LeadWithListInputPlugin()
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        List<String> extractedFields = task.getExtractedFields();

        Iterable<ObjectNode> listsToRequest;
        if (isUserInputLists(task)) {
            final String[] idsStr = StringUtils.split(task.getListIds().get(), ID_LIST_SEPARATOR_CHAR);
            Function<Set<String>, Iterable<ObjectNode>> getListIds = marketoService::getListsByIds;
            listsToRequest = super.getObjectsByIds(idsStr, getListIds);
        }
        else {
            listsToRequest = marketoService.getLists();
        }

        // Remove LIST_ID_COLUMN_NAME when sent fields to Marketo since LIST_ID_COLUMN_NAME are added by plugin code
        extractedFields.remove(MarketoUtils.LIST_ID_COLUMN_NAME);
        return FluentIterable.from(marketoService.getAllListLead(extractedFields, listsToRequest)).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    private boolean isUserInputLists(PluginTask task)
    {
        return task.getListIds().isPresent() && StringUtils.isNotBlank(task.getListIds().get());
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
