package org.embulk.input.marketo.bulk_extract;

import org.embulk.input.marketo.CsvTokenizer;
import org.embulk.spi.DataException;
import org.embulk.util.text.LineDecoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class CsvRecordIterator<T extends CsvTokenizer.PluginTask> implements Iterator<Map<String, String>>
{
    private final CsvTokenizer tokenizer;
    private final List<String> headers;
    private Map<String, String> currentCsvRecord;

    public CsvRecordIterator(LineDecoder lineDecoder, T task)
    {
        tokenizer = new CsvTokenizer(lineDecoder, task);
        if (!tokenizer.nextFile()) {
            throw new DataException("Can't read extract input stream");
        }
        headers = new ArrayList<>();
        tokenizer.nextRecord();
        while (tokenizer.hasNextColumn()) {
            headers.add(tokenizer.nextColumn());
        }
    }

    @Override
    public boolean hasNext()
    {
        if (currentCsvRecord == null) {
            currentCsvRecord = getNextCSVRecord();
        }
        return currentCsvRecord != null;
    }

    @Override
    public Map<String, String> next()
    {
        try {
            if (hasNext()) {
                return currentCsvRecord;
            }
        }
        finally {
            currentCsvRecord = null;
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
    private Map<String, String> getNextCSVRecord()
    {
        if (!tokenizer.nextRecord()) {
            return null;
        }
        Map<String, String> kvMap = new HashMap<>();
        try {
            int i = 0;
            while (tokenizer.hasNextColumn()) {
                kvMap.put(headers.get(i), tokenizer.nextColumnOrNull());
                i++;
            }
        }
        catch (CsvTokenizer.InvalidValueException ex) {
            throw new DataException("Encounter exception when parse csv file. Please check to see if you are using the correct" +
                    "quote or escape character.", ex);
        }
        return kvMap;
    }
}
