package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class ActivityBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<ActivityBulkExtractInputPlugin.PluginTask>
{
    public static final String INCREMENTAL_COLUMN = "activityDate";
    public static final String UID_COLUMN = "marketoGUID";

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask
    {
        @Config("activity_type_ids")
        @ConfigDefault("[]")
        List<String> getActivityTypeIds();

        @Config("act_type_ids")
        @ConfigDefault("[]")
        List<Integer> getActTypeIds();

        void setActTypeIds(List<Integer> activityIds);
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        task.setIncrementalColumn(Optional.of(INCREMENTAL_COLUMN));
        task.setUidColumn(Optional.of(UID_COLUMN));
        if (!task.getActivityTypeIds().isEmpty()) {
            List<Integer> activityIds = checkValidActivityTypeIds(task);

            // check input with values from server
            try (MarketoRestClient restClient = createMarketoRestClient(task)) {
                MarketoService marketoService = new MarketoServiceImpl(restClient);
                Iterable<ObjectNode> nodes = marketoService.getActivityTypes();
                if (nodes != null) {
                    checkValidActivityTypeIds(nodes, activityIds);
                }
                // ignorable if unable to get activity type ids. If thing gone wrong, the bulk extract will throw errors
            }

            // task will use getActTypeIds instead of getActivityTypeIds method
            task.setActTypeIds(activityIds);
        }
        super.validateInputTask(task);
    }

    /**
     * Check if user input activity_type_ids valid
     * @return values transformed to array of Integer
     */
    private List<Integer> checkValidActivityTypeIds(PluginTask task)
    {
        List<String> invalidIds = new ArrayList<>();
        for (String id : task.getActivityTypeIds()) {
            if (StringUtils.isBlank(id) || !StringUtils.isNumeric(StringUtils.trimToEmpty(id))) {
                invalidIds.add(id);
            }
        }

        if (!invalidIds.isEmpty()) {
            throw new ConfigException(MessageFormat.format("Invalid activity type id: [{0}]", StringUtils.join(invalidIds, ", ")));
        }

        // transform and set
        List<Integer> activityIds = new ArrayList<>();
        for (String id : task.getActivityTypeIds()) {
            activityIds.add(Integer.valueOf(StringUtils.trimToEmpty(id)));
        }

        return activityIds;
    }

    @VisibleForTesting
    protected void checkValidActivityTypeIds(Iterable<ObjectNode> nodes, List<Integer> activityIds)
    {
        Iterator<ObjectNode> it = nodes.iterator();

        List<Integer> inputIds = new ArrayList<>(activityIds);

        while (it.hasNext()) {
            ObjectNode node = it.next();
            int id = node.get("id").asInt(0);
            if (id > 0) {
                inputIds.remove(Integer.valueOf(id));
            }
        }

        if (!inputIds.isEmpty()) {
            throw new ConfigException(MessageFormat.format("Invalid activity type ids: [{0}], Available activity types: \n{1}",
                    StringUtils.join(inputIds, ", "),
                    buildActivityIdNameInfo(nodes)));
        }
    }

    private String buildActivityIdNameInfo(Iterable<ObjectNode> nodes)
    {
        Iterator<ObjectNode> it = nodes.iterator();
        StringBuilder messageBuilder = new StringBuilder();
        while (it.hasNext()) {
            ObjectNode node = it.next();
            int id = node.get("id").asInt(0);
            String name = node.get("name").asText("");
            if (id > 0) {
                messageBuilder.append("- activity id: ");
                messageBuilder.append(id);
                messageBuilder.append(", name: ");
                messageBuilder.append(name);
                messageBuilder.append("\n");
            }
        }

        return messageBuilder.toString();
    }

    @Override
    protected InputStream getExtractedStream(MarketoService service, PluginTask task, OffsetDateTime fromDate, OffsetDateTime toDate)
    {
        try {
            return new FileInputStream(service.extractAllActivity(task.getActTypeIds(), Date.from(fromDate.toInstant()),
                    Date.from(toDate.toInstant()), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("Exception when trying to extract activity", e);
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
