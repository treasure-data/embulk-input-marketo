package org.embulk.input.marketo.delegate;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.ServiceResponseMapperBuildable;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by tai.khuu on 5/21/18.
 */
public class LeadServiceResponseMapperBuilder<T extends LeadServiceResponseMapperBuilder.PluginTask> implements ServiceResponseMapperBuildable<T>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MarketoService marketoService;

    private final T pluginTask;

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
        @Config("included_fields")
        @ConfigDefault("null")
        Optional<List<String>> getIncludedLeadFields();

        @Config("extracted_fields")
        @ConfigDefault("[]")
        List<String> getExtractedFields();

        void setExtractedFields(List<String> extractedFields);
    }

    public LeadServiceResponseMapperBuilder(T task, MarketoService marketoService)
    {
        this.pluginTask = task;
        this.marketoService = marketoService;
    }

    protected List<MarketoField> getLeadColumns()
    {
        List<MarketoField> columns = marketoService.describeLead();
        if (pluginTask.getIncludedLeadFields().isPresent() && !pluginTask.getIncludedLeadFields().get().isEmpty()) {
            List<MarketoField> filteredColumns = new ArrayList<>();
            List<String> includedFields = pluginTask.getIncludedLeadFields().get();
            for (String fieldName : includedFields) {
                Optional<MarketoField> includedField = lookupFieldIgnoreCase(columns, fieldName);
                if (includedField.isPresent()) {
                    filteredColumns.add(includedField.get());
                }
                else {
                    logger.warn("Included field [{}] not found in Marketo lead field", fieldName);
                }
            }
            columns = filteredColumns;
            logger.info("Included Fields option is set, included columns: [{}]", columns);
        }
        return columns;
    }

    private static Optional<MarketoField> lookupFieldIgnoreCase(List<MarketoField> inputList, String lookupFieldName)
    {
        for (MarketoField marketoField : inputList) {
            if (marketoField.getName().equalsIgnoreCase(lookupFieldName)) {
                return Optional.of(marketoField);
            }
        }
        return Optional.empty();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(T task)
    {
        List<MarketoField> leadColumns = getLeadColumns();
        pluginTask.setExtractedFields(MarketoUtils.getFieldNameFromMarketoFields(leadColumns));
        return MarketoUtils.buildDynamicResponseMapper(pluginTask.getSchemaColumnPrefix(), leadColumns);
    }
}
