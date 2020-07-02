package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        @Config("custom_object_filter_values")
        @ConfigDefault("\"\"")
        String getCustomObjectFilterValues();
    }

    public CustomObjectInputPlugin()
    {
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        super.validateInputTask(task);
        if (StringUtils.isBlank(task.getCustomObjectFilterType())) {
            throw new ConfigException("`custom_object_filter_type` cannot be empty");
        }
        if (StringUtils.isBlank(task.getCustomObjectAPIName())) {
            throw new ConfigException("`custom_object_api_name` cannot be empty");
        }
        if (StringUtils.isBlank(task.getCustomObjectFilterValues())) {
            if (task.getToValue().isPresent() && !isValidFilterRange(task)) {
                throw new ConfigException(String.format("`to_value` (%s) cannot be less than the `from_value` (%s)", task.getToValue().get(), task.getFromValue()));
            }
        }
        else if (refineFilterValues(task.getCustomObjectFilterValues()).isEmpty()) {
            throw new ConfigException("`custom_object_filter_values` cannot contain empty values only");
        }
    }

    private Set<String> refineFilterValues(String filterValues)
    {
        return Stream.of(StringUtils.split(filterValues, ",")).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
    }

    private boolean isValidFilterRange(PluginTask task)
    {
        return task.getToValue().get() > task.getFromValue();
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        Iterable<ObjectNode> responseObj;
        if (StringUtils.isNotBlank(task.getCustomObjectFilterValues())) {
            Set<String> refinedValues = refineFilterValues(task.getCustomObjectFilterValues());
            responseObj = marketoService.getCustomObject(task.getCustomObjectAPIName(), task.getCustomObjectFilterType(), refinedValues, task.getCustomObjectFields().orNull());
        }
        else {
            // When `to_value` is not set, will try to import all consecutive custom objects started from `from_value`
            responseObj = marketoService.getCustomObject(task.getCustomObjectAPIName(), task.getCustomObjectFilterType(), task.getCustomObjectFields().orNull(), task.getFromValue(), task.getToValue().orNull());
        }

        return FluentIterable.from(filterInvalidRecords(responseObj)).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    /**
     * Marketo include error in invalid records. This method will filter those records and print it to output log
     * @param resultIt
     * @return
     */
    private Iterable<ObjectNode> filterInvalidRecords(Iterable<ObjectNode> resultIt)
    {
        return Iterables.filter(resultIt, (obj) -> {
            if (obj.has("reasons")) {
                logger.warn(obj.get("reasons").toString());
                return false;
            }
            return true;
        });
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
