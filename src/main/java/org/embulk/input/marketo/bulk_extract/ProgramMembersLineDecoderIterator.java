package org.embulk.input.marketo.bulk_extract;

import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.delegate.ProgramMembersBulkExtractInputPlugin;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.Exec;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.text.LineDecoder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ProgramMembersLineDecoderIterator implements Iterator<LineDecoder>, AutoCloseable
{
    private LineDecoder currentLineDecoder;
    private final Iterator<Integer> programs;
    private final MarketoService marketoService;
    private final MarketoRestClient marketoRestClient;
    private final ProgramMembersBulkExtractInputPlugin.PluginTask task;

    public ProgramMembersLineDecoderIterator(Iterator<Integer> programs, ProgramMembersBulkExtractInputPlugin.PluginTask task, MarketoRestClient marketoRestClient)
    {
        this.marketoRestClient = marketoRestClient;
        this.marketoService = new MarketoServiceImpl(marketoRestClient);
        this.programs = programs;
        this.task = task;
    }

    @Override
    public void close()
    {
        if (currentLineDecoder != null) {
            currentLineDecoder.close();
        }
        if (marketoRestClient != null) {
            marketoRestClient.close();
        }
    }

    @Override
    public boolean hasNext()
    {
        return programs.hasNext();
    }

    @Override
    public LineDecoder next()
    {
        if (hasNext()) {
            int programId = programs.next();
            InputStream extractedStream = getExtractedStream(marketoService, task, programId);
            currentLineDecoder = LineDecoder.of(new InputStreamFileInput(Exec.getBufferAllocator(), extractedStream), StandardCharsets.UTF_8, null);
            return currentLineDecoder;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("Removed are not supported");
    }

    private InputStream getExtractedStream(MarketoService service, ProgramMembersBulkExtractInputPlugin.PluginTask task, int programId)
    {
        try {
            List<String> fieldNames = new ArrayList<>(task.getProgramMemberFields().keySet());
            return new FileInputStream(service.extractProgramMembers(
                    fieldNames, programId, task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
    }
}
