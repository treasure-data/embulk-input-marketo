package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class CustomObjectInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private CustomObjectInputPlugin customObjectInputPlugin;

    private ConfigSource configSource;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        customObjectInputPlugin = Mockito.spy(new CustomObjectInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/custom_object_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(customObjectInputPlugin).createMarketoRestClient(Mockito.any(CustomObjectInputPlugin.PluginTask.class));
    }

    @Test(expected = ConfigException.class)
    public void validateCustomObjectFilterTypeError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = Mockito.mock(CustomObjectInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getCustomObjectFilterType()).thenReturn("");
        customObjectInputPlugin.validateInputTask(pluginTask);
    }

    @Test(expected = ConfigException.class)
    public void validateCustomObjectAPINameError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = Mockito.mock(CustomObjectInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getCustomObjectAPIName()).thenReturn("");
        customObjectInputPlugin.validateInputTask(pluginTask);
    }

    @Test(expected = ConfigException.class)
    public void validateFromValueGreaterThanToValueError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = Mockito.mock(CustomObjectInputPlugin.PluginTask.class);
        Mockito.when(pluginTask.getFromValue()).thenReturn(100);
        Mockito.when(pluginTask.getToValue()).thenReturn(Optional.of(90));
        customObjectInputPlugin.validateInputTask(pluginTask);
    }

    @Test
    public void testRun() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_response_full.json"), javaType);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_describe_marketo_fields_full.json"), marketoFieldsType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        Mockito.when(mockMarketoRestClient.getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(mockRecordPagingIterable);
        Mockito.when(mockMarketoRestClient.describeCustomObject(Mockito.anyString())).thenReturn(marketoFields);
        CustomObjectInputPlugin.PluginTask task = configSource.loadConfig(CustomObjectInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).describeCustomObject(Mockito.anyString());
        Schema embulkSchema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockPageBuilder, Mockito.times(3)).setLong(Mockito.eq(embulkSchema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1L, 2L, 3L}, allValues.toArray());
    }
}
