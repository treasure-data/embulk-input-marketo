package org.embulk.input.marketo.delegate;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class ActivityBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<ActivityBulkExtractInputPlugin.PluginTask>
{
    private static final Logger LOGGER = Exec.getLogger(ActivityBulkExtractInputPlugin.class);
    public static final String INCREMENTAL_COLUMN = "activityDate";
    public static final String UID_COLUMN = "marketoGUID";

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask {}

    @Override
    public void validateInputTask(PluginTask task)
    {
        task.setIncrementalColumn(INCREMENTAL_COLUMN);
        task.setUidColumn(UID_COLUMN);
        super.validateInputTask(task);
    }

    @Override
    protected InputStream getExtractedStream(MarketoService service, PluginTask task, DateTime fromDate, DateTime toDate)
    {
        try {
            return new FileInputStream(service.extractAllActivity(fromDate.toDate(), toDate.toDate(), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
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
