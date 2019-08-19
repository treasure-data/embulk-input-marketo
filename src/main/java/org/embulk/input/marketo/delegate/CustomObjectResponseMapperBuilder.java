package org.embulk.input.marketo.delegate;

import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.ServiceResponseMapperBuildable;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomObjectResponseMapperBuilder<T extends CustomObjectResponseMapperBuilder.PluginTask> implements ServiceResponseMapperBuildable<T>
{
    private static final Logger LOGGER = Exec.getLogger(CustomObjectResponseMapperBuilder.class);
    private MarketoService marketoService;

    private T pluginTask;

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
            List<String> includedFields = Arrays.asList(pluginTask.getCustomObjectFields().get().split(","));
            for (String fieldName : includedFields) {
                Optional<MarketoField> includedField = lookupFieldIgnoreCase(columns, fieldName);
                if (includedField.isPresent()) {
                    filteredColumns.add(includedField.get());
                }
                else {
                    LOGGER.warn("Included field [{}] not found in Marketo Custom Object [{}] field", fieldName, pluginTask.getCustomObjectAPIName());
                }
            }
            columns = filteredColumns;
            LOGGER.info("Included Fields option is set, included columns: [{}]", columns);
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
        return Optional.absent();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(T task)
    {
        List<MarketoField> customObjectColumns = getCustomObjectColumns();
        return MarketoUtils.buildDynamicResponseMapper(pluginTask.getSchemaColumnPrefix(), customObjectColumns);
    }
}
