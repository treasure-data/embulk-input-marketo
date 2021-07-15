package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.MarketoService;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.embulk.input.marketo.delegate.MarketoBaseInputPluginDelegate.ID_LIST_SEPARATOR_CHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class MarketoBaseInputPluginDelegateTest
{
    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();
    private final ObjectMapper mapper = new ObjectMapper();
    private final MarketoService service = mock(MarketoService.class);
    MarketoBaseInputPluginDelegate delegate = spy(MarketoBaseInputPluginDelegate.class);

    @Test
    public void testInputIds()
    {
        doReturn(getSampleResp(), Collections.emptyList(), Collections.emptyList()).when(service).getListsByIds(anySet());
        Function<Set<String>, Iterable<ObjectNode>> getListIds = service::getListsByIds;

        final String[] ids = StringUtils.split("123,abc,,123.45,1002 ", ID_LIST_SEPARATOR_CHAR);
        Iterable<ObjectNode> rs1 = delegate.getObjectsByIds(ids, getListIds);
        assertEquals(Lists.newArrayList(rs1).size(), 1);

        final String[] nullIds = StringUtils.split("  , ,  ,, ", ID_LIST_SEPARATOR_CHAR);
        ConfigException exception = assertThrows(ConfigException.class, () -> delegate.getObjectsByIds(nullIds, getListIds));
        assertEquals("No valid Id specified", exception.getMessage());

        final String[] notExist = StringUtils.split(" 123 ,3453 ,  234234,,", ID_LIST_SEPARATOR_CHAR);
        exception = assertThrows(ConfigException.class, () -> delegate.getObjectsByIds(notExist, getListIds));
        assertEquals("No valid Id found", exception.getMessage());
    }

    private Iterable<ObjectNode> getSampleResp()
    {
        List<ObjectNode> objects = new ArrayList<>();
        objects.add(mapper.createObjectNode().put("id", 1002));
        return objects;
    }
}
