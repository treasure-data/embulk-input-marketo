package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadWithProgramInputPlugin extends MarketoBaseInputPluginDelegate<LeadWithProgramInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, LeadServiceResponseMapperBuilder.PluginTask
    {
        @Config("program_ids")
        @ConfigDefault("null")
        Optional<String> getProgramIds();

        @Config("skip_invalid_program_id")
        @ConfigDefault("false")
        boolean getSkipInvalidProgram();
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        List<String> fieldNames = task.getExtractedFields();

        Iterable<ObjectNode> requestProgs;
        if (isUserInputProgs(task)) {
            final String[] idsStr = StringUtils.split(task.getProgramIds().get(), ID_LIST_SEPARATOR_CHAR);
            Function<Set<String>, Iterable<ObjectNode>> getListIds = (ids) -> marketoService.getProgramsByIds(ids);
            requestProgs = super.getObjectsByIds(idsStr, task.getSkipInvalidProgram(), getListIds);
        }
        else {
            requestProgs = marketoService.getPrograms();
        }

        // Remove PROGRAM_ID_COLUMN_NAME when sent fields to Marketo since PROGRAM_ID_COLUMN_NAME are added by plugin code
        fieldNames.remove(MarketoUtils.PROGRAM_ID_COLUMN_NAME);
        return FluentIterable.from(marketoService.getAllProgramLead(fieldNames, requestProgs)).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    private boolean isUserInputProgs(LeadWithProgramInputPlugin.PluginTask task)
    {
        return task.getProgramIds().isPresent() && StringUtils.isNotBlank(task.getProgramIds().get());
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
