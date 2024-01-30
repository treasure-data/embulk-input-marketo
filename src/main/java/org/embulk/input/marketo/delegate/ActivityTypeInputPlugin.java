package org.embulk.input.marketo.delegate;

import com.google.common.collect.FluentIterable;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.type.Types;

import java.util.Iterator;

public class ActivityTypeInputPlugin extends MarketoBaseInputPluginDelegate<ActivityTypeInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
    }

    public ActivityTypeInputPlugin()
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        return FluentIterable.from(marketoService.getActivityTypes()).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        builder.add("id", Types.LONG)
                .add("name", Types.STRING)
                .add("description", Types.STRING)
                .add("primaryAttribute", Types.JSON)
                .add("attributes", Types.JSON)
                .add("apiName", Types.STRING);
        return builder.build();
    }
}
