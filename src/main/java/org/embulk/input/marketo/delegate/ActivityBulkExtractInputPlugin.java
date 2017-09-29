package org.embulk.input.marketo.delegate;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Date;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class ActivityBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<ActivityBulkExtractInputPlugin.PluginTask>
{
    private static final Logger LOGGER = Exec.getLogger(ActivityBulkExtractInputPlugin.class);

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask {}

    public ActivityBulkExtractInputPlugin()
    {
        super("activityDate", "marketoGUID");
    }

    @Override
    protected InputStream getExtractedStream(PluginTask task, Schema schema)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            Date fromDate = task.getFromDate();
            return new FileInputStream(marketoService.extractAllActivity(fromDate, task.getToDate().orNull(), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
        }
        catch (FileNotFoundException e) {
            LOGGER.error("Exception when trying to extract activity", e);
            throw new DataException("Error when trying to extract activity");
        }
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        builder.add("marketoGUID", Types.STRING)
                .add("leadId", Types.STRING)
                .add("activityDate", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("activityTypeId", Types.STRING)
                .add("campaignId", Types.STRING)
                .add("primaryAttributeValueId", Types.STRING)
                .add("primaryAttributeValue", Types.STRING)
                .add("attributes", Types.JSON);
        return builder.build();
    }
}
