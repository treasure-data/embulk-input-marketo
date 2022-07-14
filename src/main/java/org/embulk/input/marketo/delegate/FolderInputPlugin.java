package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.jackson.JacksonServiceResponseMapper;
import org.embulk.base.restclient.record.ServiceRecord;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.spi.type.Types;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class FolderInputPlugin extends MarketoBaseInputPluginDelegate<FolderInputPlugin.PluginTask>
{
    public interface PluginTask extends MarketoBaseInputPluginDelegate.PluginTask
    {
        @Config("root_id")
        @ConfigDefault("null")
        Optional<Long> getRootId();

        @Config("root_type")
        @ConfigDefault("\"folder\"")
        RootType getRootType();

        @Config("max_depth")
        @ConfigDefault("2")
        Integer getMaxDepth();

        @Config("workspace")
        @ConfigDefault("null")
        Optional<String> getWorkspace();
    }

    public FolderInputPlugin()
    {
    }

    @Override
    protected Iterator<ServiceRecord> getServiceRecords(MarketoService marketoService, PluginTask task)
    {
        return StreamSupport.stream(marketoService.getFolders(
                task.getRootId(),
                task.getRootType(),
                task.getMaxDepth(),
                task.getWorkspace()
        ).spliterator(), false).map(MarketoUtils.TRANSFORM_OBJECT_TO_JACKSON_SERVICE_RECORD_FUNCTION::apply).iterator();
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        JacksonServiceResponseMapper.Builder builder = JacksonServiceResponseMapper.builder();
        builder.add("id", Types.LONG)
                .add("name", Types.STRING)
                .add("description", Types.STRING)
                .add("createdAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("updatedAt", Types.TIMESTAMP, MarketoUtils.MARKETO_DATE_TIME_FORMAT)
                .add("url", Types.STRING)
                .add("folderId", Types.JSON)
                .add("folderType", Types.STRING)
                .add("parent", Types.JSON)
                .add("path", Types.STRING)
                .add("isArchive", Types.BOOLEAN)
                .add("isSystem", Types.BOOLEAN)
                .add("accessZoneId", Types.LONG)
                .add("workspace", Types.STRING);
        return builder.build();
    }

    public enum RootType {
        FOLDER,
        PROGRAM;

        @JsonCreator
        public static RootType of(String value)
        {
            return RootType.valueOf(value.toUpperCase());
        }
    }
}
