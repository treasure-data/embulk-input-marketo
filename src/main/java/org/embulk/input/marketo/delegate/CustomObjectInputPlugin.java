package org.embulk.input.marketo.delegate;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import java.util.Iterator;

public class CustomObjectInputPlugin extends MarketoBaseInputPluginDelegate<CustomObjectInputPlugin.PluginTask>
{
    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask, CustomObjectResponseMapperBuilder.PluginTask
    {
        @Config("custom_object_filter_type")
        @ConfigDefault("\"\"")
        String getCustomObjectFilterType();

        @Config("custom_object_filter_from_value")
        @ConfigDefault("1")
        Integer getFromValue();

        @Config("custom_object_filter_to_value")
        @ConfigDefault("null")
        Optional<Integer> getToValue();
    }

    public CustomObjectInputPlugin()
    {
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        if (StringUtils.isBlank(task.getCustomObjectFilterType())) {
            throw new ConfigException("`custom_object_filter_type` cannot empty");
        }
        if (StringUtils.isBlank(task.getCustomObjectAPIName())) {
            throw new ConfigException("`custom_object_api_name` cannot empty");
        }
        if (task.getToValue().isPresent() && task.getToValue().get() < task.getFromValue()) {
            throw new ConfigException(String.format("`to_value` (%s) cannot less than  the `from_value` (%s)", task.getToValue().get(), task.getFromValue()));
        }
    }
    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        return FluentIterable
                .from(marketoService.getCustomObject(task.getCustomObjectAPIName(), task.getCustomObjectFilterType(), task.getCustomObjectFields().orNull(), task.getFromValue(), task.getToValue().orNull()))
                .transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            CustomObjectResponseMapperBuilder<PluginTask> customObjectResponseMapperBuilder = new CustomObjectResponseMapperBuilder<>(task, marketoService);
            return customObjectResponseMapperBuilder.buildServiceResponseMapper(task);
        }
    }

}
