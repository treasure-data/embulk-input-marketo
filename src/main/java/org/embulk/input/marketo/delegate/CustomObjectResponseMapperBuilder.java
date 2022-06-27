package org.embulk.input.marketo.delegate;

import org.apache.commons.lang3.StringUtils;
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

public class CustomObjectResponseMapperBuilder<T extends CustomObjectResponseMapperBuilder.PluginTask> implements ServiceResponseMapperBuildable<T>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MarketoService marketoService;

    private final T pluginTask;

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
        @Config("custom_object_api_name")
        @ConfigDefault("\"\"")
        String getCustomObjectAPIName();

        @Config("custom_object_fields")
        @ConfigDefault("null")
        Optional<String> getCustomObjectFields();
    }

    public CustomObjectResponseMapperBuilder(T task, MarketoService marketoService)
    {
        this.pluginTask = task;
        this.marketoService = marketoService;
    }

    protected List<MarketoField> getCustomObjectColumns()
    {
        List<MarketoField> columns = marketoService.describeCustomObject(pluginTask.getCustomObjectAPIName());
        if (pluginTask.getCustomObjectFields().isPresent() && StringUtils.isNotBlank(pluginTask.getCustomObjectFields().get())) {
            List<MarketoField> filteredColumns = new ArrayList<>();
            String[] includedFields = pluginTask.getCustomObjectFields().get().split(",");
            for (String fieldName : includedFields) {
                Optional<MarketoField> includedField = lookupFieldIgnoreCase(columns, fieldName);
                if (includedField.isPresent()) {
                    filteredColumns.add(includedField.get());
                }
                else {
                    logger.warn("Included field [{}] not found in Marketo Custom Object [{}] field", fieldName, pluginTask.getCustomObjectAPIName());
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
        List<MarketoField> customObjectColumns = getCustomObjectColumns();
        return MarketoUtils.buildDynamicResponseMapper(pluginTask.getSchemaColumnPrefix(), customObjectColumns);
    }
}
