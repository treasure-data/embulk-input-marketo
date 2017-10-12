package org.embulk.input.marketo.model;

/**
 * Created by tai.khuu on 10/12/17.
 */
public class BulkExtractRangeHeader
{
    private Long start;
    private Long end;

    public BulkExtractRangeHeader(long start)
    {
        this.start = start;
    }

    public BulkExtractRangeHeader(long start, long end)
    {
        this.start = start;
        this.end = end;
    }

    public String toRangeHeaderValue(){
        return "bytes=" + start + "-" + (end != null ? end : "");
    }
}
