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

/**
 * Input plugin use to import Campaign
 * Created by tai.khuu on 9/18/17.
 */
public class CampaignInputPlugin extends MarketoBaseInputPluginDelegate<CampaignInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
    }

    public CampaignInputPlugin()
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        return FluentIterable.from(marketoService.getCampaign()).transform(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        builder.add("id", Types.LONG)
                .add("name", Types.STRING)
                .add("description", Types.STRING)
                .add("type", Types.STRING)
                .add("workspaceName", Types.STRING)
                .add("createdAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("updatedAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("active", Types.BOOLEAN);
        return builder.build();
    }
}
