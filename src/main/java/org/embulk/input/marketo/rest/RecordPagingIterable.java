package org.embulk.input.marketo.rest;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Record Iterable class that will go through Marketo Paging
 * Warning this iterator implementation do not cached page due to reduce memory usage. So iterate through a
 * RecordIterate multiple time is not recommended since it will sent query to Marketo on every call.
 * Created by tai.khuu on 9/5/17.
 */
public class RecordPagingIterable<T> implements Iterable<T>
{
    private PagingFunction pagingFunction;

    public RecordPagingIterable(PagingFunction pagingFunction)
    {
        this.pagingFunction = pagingFunction;
    }

    @Override
    public Iterator<T> iterator()
    {
        return this.new RecordIterator();
    }

    private class RecordIterator implements Iterator<T>
    {
        Page currentPage;
        private Iterator<T> currentIterator;

        public RecordIterator()
        {
            currentPage = pagingFunction.getFirstPage();
            this.currentIterator = currentPage.getRecords().iterator();
        }

        @Override
        public boolean hasNext()
        {
            return currentIterator.hasNext() || currentPage.hasNext;
        }

        @Override
        public T next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("Call next on an empty iterator");
            }
            if (!currentIterator.hasNext()) {
                currentPage = pagingFunction.getNextPage(currentPage);
                currentIterator = currentPage.getRecords().iterator();
            }
            return currentIterator.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("RecordIterator not support remove");
        }
    }

    public interface PagingFunction<P extends Page>
    {
        P getNextPage(P currentPage);
        /**
         * All implementation must make sure calling get first page multiple time should always return.
         * @return P
         */
        P getFirstPage();
    }

    public static class Page<T>
    {
        private Iterable<T> records;

        private boolean hasNext;

        public Page(Iterable<T> records, boolean hasNext)
        {
            this.records = records;
            this.hasNext = hasNext;
        }

        public Iterable<T> getRecords()
        {
            return records;
        }

        public void setRecords(List<T> records)
        {
            this.records = records;
        }

        public boolean isHasNext()
        {
            return hasNext;
        }

        public void setHasNext(boolean hasNext)
        {
            this.hasNext = hasNext;
        }
    }

    public static class MarketoPage<T> extends Page<T>
    {
        private String nextPageToken;

        public MarketoPage(Iterable<T> records, String nextPageToken, boolean moreResult)
        {
            super(records, moreResult);
            this.nextPageToken = nextPageToken;
        }

        public String getNextPageToken()
        {
            return nextPageToken;
        }

        public void setNextPageToken(String nextPageToken)
        {
            this.nextPageToken = nextPageToken;
        }
    }
}
