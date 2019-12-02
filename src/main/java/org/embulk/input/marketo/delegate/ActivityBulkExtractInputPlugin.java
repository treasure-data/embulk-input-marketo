package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Types;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class ActivityBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<ActivityBulkExtractInputPlugin.PluginTask>
{
    private static final Logger LOGGER = Exec.getLogger(ActivityBulkExtractInputPlugin.class);
    public static final String INCREMENTAL_COLUMN = "activityDate";
    public static final String UID_COLUMN = "marketoGUID";

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask
    {
        @Config("activity_type_ids")
        @ConfigDefault("[]")
        List<String> getActivityTypeIds();
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        task.setIncrementalColumn(Optional.of(INCREMENTAL_COLUMN));
        task.setUidColumn(Optional.of(UID_COLUMN));
        if (!task.getActivityTypeIds().isEmpty()) {
            List<String> invalidIds = new ArrayList<>();
            for (String id : task.getActivityTypeIds()) {
                if (StringUtils.isBlank(id) || !StringUtils.isNumeric(StringUtils.trimToEmpty(id))) {
                    invalidIds.add(id);
                }
            }

            if (!invalidIds.isEmpty()) {
                throw new ConfigException(MessageFormat.format("Invalid activity type id: [{0}]", StringUtils.join(invalidIds, ", ")));
            }

            try (MarketoRestClient restClient = createMarketoRestClient(task)) {
                MarketoService marketoService = new MarketoServiceImpl(restClient);
                Iterable<ObjectNode> nodes = marketoService.getActivityTypes();
                if (nodes != null) {
                    checkValidActivityTypeIds(nodes, task.getActivityTypeIds());
                }
                else {
                    // ignore since unable to get activity type ids. If thing gone wrong. the bulk extract will throw errors
                }
            }
        }
        super.validateInputTask(task);
    }

    private void checkValidActivityTypeIds(Iterable<ObjectNode> nodes, List<String> inputActivityTypeIds)
    {
        Iterator<ObjectNode> it = nodes.iterator();

        List<String> inputIds = new ArrayList<>(inputActivityTypeIds);

        while (it.hasNext()) {
            ObjectNode node = it.next();
            int id = node.get("id").asInt(0);
            if (id > 0) {
                inputIds.remove(String.valueOf(id));
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
                messageBuilder.append(String.valueOf(id));
                messageBuilder.append(", name: ");
                messageBuilder.append(name);
                messageBuilder.append("\n");
            }
        }

        return messageBuilder.toString();
    }

    @Override
    protected InputStream getExtractedStream(MarketoService service, PluginTask task, DateTime fromDate, DateTime toDate)
    {
        try {
            List<String> inputIds = task.getActivityTypeIds();
            List<Integer> activityTypeIds = new ArrayList<>();
            for (String id : inputIds) {
                activityTypeIds.add(Integer.valueOf(id));
            }
            return new FileInputStream(service.extractAllActivity(activityTypeIds, fromDate.toDate(), toDate.toDate(), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
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
