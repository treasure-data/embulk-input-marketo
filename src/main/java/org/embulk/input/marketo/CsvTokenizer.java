package org.embulk.input.marketo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.embulk.config.ConfigException;
import org.embulk.spi.DataException;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.embulk.util.text.LineDecoder;
import org.embulk.util.text.Newline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * Created by tai.khuu on 9/15/17.
 */
public class CsvTokenizer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvTokenizer.class);

    enum RecordState
    {
        NOT_END, END,
    }

    enum ColumnState
    {
        BEGIN, VALUE, QUOTED_VALUE, AFTER_QUOTED_VALUE, FIRST_TRIM, LAST_TRIM_OR_VALUE,
    }

    private static final char END_OF_LINE = '\0';
    static final char NO_QUOTE = '\0';
    static final char NO_ESCAPE = '\0';

    public interface PluginTask extends Task
    {
        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        Charset getCharset();

        @Config("newline")
        @ConfigDefault("\"CRLF\"")
        Newline getNewline();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        String getDelimiter();

        @Config("quote")
        @ConfigDefault("\"\\\"\"")
        Optional<QuoteCharacter> getQuoteChar();

        @Config("escape")
        @ConfigDefault("\"\\\\\"")
        Optional<EscapeCharacter> getEscapeChar();

        // Null value handling: if the CsvParser found 'non-quoted empty string's,
        // it replaces them to string that users specified like "\N", "NULL".
        @Config("null_string")
        @ConfigDefault("\"null\"")
        Optional<String> getNullString();

        @Config("trim_if_not_quoted")
        @ConfigDefault("false")
        boolean getTrimIfNotQuoted();

        @Config("max_quoted_size_limit")
        @ConfigDefault("131072") //128kB
        long getMaxQuotedSizeLimit();

        @Config("comment_line_marker")
        @ConfigDefault("null")
        Optional<String> getCommentLineMarker();
    }

    private final char delimiterChar;
    private final String delimiterFollowingString;
    private final char quote;
    private final char escape;
    private final String newline;
    private final boolean trimIfNotQuoted;
    private final long maxQuotedSizeLimit;
    private final String commentLineMarker;
    private final LineDecoder input;
    private final String nullStringOrNull;

    private RecordState recordState = RecordState.END;  // initial state is end of a record. nextRecord() must be called first
    private long lineNumber = 0;

    private String line = null;
    private int linePos = 0;
    private boolean wasQuotedColumn = false;
    private final List<String> quotedValueLines = new ArrayList<>();
    private final Deque<String> unreadLines = new ArrayDeque<>();

    public CsvTokenizer(LineDecoder input, PluginTask task)
    {
        this(task.getDelimiter(), task.getQuoteChar().orElse(QuoteCharacter.noQuote()).getCharacter(),
                task.getEscapeChar().orElse(EscapeCharacter.noEscape()).getCharacter(), task.getNewline().getString(),
                task.getTrimIfNotQuoted(), task.getMaxQuotedSizeLimit(), task.getCommentLineMarker().orElse(null), input, task.getNullString().orElse(null));
    }

    public CsvTokenizer(String delimiter, char quote, char escape, String newline, boolean trimIfNotQuoted, long maxQuotedSizeLimit, String commentLineMarker, LineDecoder input, String nullStringOrNull)
    {
        if (delimiter.length() == 0) {
            throw new ConfigException("Empty delimiter is not allowed");
        }
        else {
            this.delimiterChar = delimiter.charAt(0);
            if (delimiter.length() > 1) {
                delimiterFollowingString = delimiter.substring(1);
            }
            else {
                delimiterFollowingString = null;
            }
        }
        this.quote = quote;
        this.escape = escape;
        this.newline = newline;
        this.trimIfNotQuoted = trimIfNotQuoted;
        this.maxQuotedSizeLimit = maxQuotedSizeLimit;
        this.commentLineMarker = commentLineMarker;
        this.input = input;
        this.nullStringOrNull = nullStringOrNull;
    }

    private Reader inputStream;

    public CsvTokenizer(Reader inputStream)
    {
        this.inputStream = inputStream;
        this.delimiterChar = 0; // Unused
        this.delimiterFollowingString = null; // Unused
        this.quote = 0; // Unused
        this.escape = 0; // Unused
        this.newline = null; // Unused;
        this.trimIfNotQuoted = false; // Unused
        this.maxQuotedSizeLimit = 0; // Unused
        this.commentLineMarker = null; // Unused
        this.input = null; // Unused
        this.nullStringOrNull = null; // Unused
    }

    public CSVParser csvParse()
    {
        try {
            String path = String.format("tmp_%d.csv", Calendar.getInstance().getTimeInMillis());
            File file = new File(path);
            FileWriter filewriter = new FileWriter(file);

            LOGGER.info("create tmp file: " + path);

            BufferedReader b = new BufferedReader(inputStream);
            String line = b.readLine();
            int count = 0;
            while (true) {
                filewriter.write(line);
                line = b.readLine();
                if (line == null) {
                    break;
                }
                filewriter.write("\r\n");
                count += 1;
                if (count % 10000 == 0) {
                    LOGGER.info("import record count: " + count);
                }
            }
            filewriter.close();
            inputStream.close();

            CSVParser csvParser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            return csvParser;
        }
        catch (IOException e) {
            throw new InvalidValueException(e.getMessage());
        }
    }

    public long getCurrentLineNumber()
    {
        return lineNumber;
    }

    public boolean skipHeaderLine()
    {
        boolean skipped = input.poll() != null;
        if (skipped) {
            lineNumber++;
        }
        return skipped;
    }

    // returns skipped line
    public String skipCurrentLine()
    {
        String skippedLine;
        if (quotedValueLines.isEmpty()) {
            skippedLine = line;
        }
        else {
            // recover lines of quoted value
            skippedLine = quotedValueLines.remove(0);  // TODO optimize performance
            unreadLines.addAll(quotedValueLines);
            lineNumber -= quotedValueLines.size();
            if (line != null) {
                unreadLines.add(line);
                lineNumber -= 1;
            }
            quotedValueLines.clear();
        }
        recordState = RecordState.END;
        return skippedLine;
    }

    public boolean nextFile()
    {
        boolean next = input.nextFile();
        if (next) {
            lineNumber = 0;
        }
        return next;
    }

    // used by guess-csv
    public boolean nextRecord()
    {
        return nextRecord(true);
    }

    public boolean nextRecord(boolean skipEmptyLine)
    {
        // If at the end of record, read the next line and initialize the state
        if (recordState != RecordState.END) {
            throw new TooManyColumnsException("Too many columns");
        }

        boolean hasNext = nextLine(skipEmptyLine);
        if (hasNext) {
            recordState = RecordState.NOT_END;
            return true;
        }
        else {
            return false;
        }
    }

    private boolean nextLine(boolean skipEmptyLine)
    {
        while (true) {
            if (!unreadLines.isEmpty()) {
                line = unreadLines.removeFirst();
            }
            else {
                line = input.poll();
                if (line == null) {
                    return false;
                }
            }
            linePos = 0;
            lineNumber++;

            boolean skip = skipEmptyLine && (
                    line.isEmpty() ||
                            (commentLineMarker != null && line.startsWith(commentLineMarker)));
            if (!skip) {
                return true;
            }
        }
    }

    public boolean hasNextColumn()
    {
        return recordState == RecordState.NOT_END;
    }

    public String nextColumn()
    {
        if (!hasNextColumn()) {
            throw new TooFewColumnsException("Too few columns");
        }

        // reset last state
        wasQuotedColumn = false;
        quotedValueLines.clear();

        // local state
        int valueStartPos = linePos;
        int valueEndPos = 0;  // initialized by VALUE state and used by LAST_TRIM_OR_VALUE and
        StringBuilder quotedValue = null;  // initial by VALUE or FIRST_TRIM state and used by QUOTED_VALUE state
        ColumnState columnState = ColumnState.BEGIN;

        while (true) {
            final char c = nextChar();

            switch (columnState) {
                case BEGIN:
                    // TODO optimization: state is BEGIN only at the first character of a column.
                    //      this block can be out of the looop.
                    if (isDelimiter(c)) {
                        // empty value
                        if (delimiterFollowingString == null) {
                            return "";
                        }
                        else if (isDelimiterFollowingFrom(linePos)) {
                            linePos += delimiterFollowingString.length();
                            return "";
                        }
                        // not a delimiter
                    }
                    if (isEndOfLine(c)) {
                        // empty value
                        recordState = RecordState.END;
                        return "";
                    }
                    else if (isSpace(c) && trimIfNotQuoted) {
                        columnState = ColumnState.FIRST_TRIM;
                    }
                    else if (isQuote(c)) {
                        valueStartPos = linePos;  // == 1
                        wasQuotedColumn = true;
                        quotedValue = new StringBuilder();
                        columnState = ColumnState.QUOTED_VALUE;
                    }
                    else {
                        columnState = ColumnState.VALUE;
                    }
                    break;

                case FIRST_TRIM:
                    if (isDelimiter(c)) {
                        // empty value
                        if (delimiterFollowingString == null) {
                            return "";
                        }
                        else if (isDelimiterFollowingFrom(linePos)) {
                            linePos += delimiterFollowingString.length();
                            return "";
                        }
                        // not a delimiter
                    }
                    if (isEndOfLine(c)) {
                        // empty value
                        recordState = RecordState.END;
                        return "";
                    }
                    else if (isQuote(c)) {
                        // column has heading spaces and quoted. TODO should this be rejected?
                        valueStartPos = linePos;
                        wasQuotedColumn = true;
                        quotedValue = new StringBuilder();
                        columnState = ColumnState.QUOTED_VALUE;
                    }
                    else if (isSpace(c)) {
                        // skip this character
                    } else {
                        valueStartPos = linePos - 1;
                        columnState = ColumnState.VALUE;
                    }
                    break;

                case VALUE:
                    if (isDelimiter(c)) {
                        if (delimiterFollowingString == null) {
                            return line.substring(valueStartPos, linePos - 1);
                        }
                        else if (isDelimiterFollowingFrom(linePos)) {
                            String value = line.substring(valueStartPos, linePos - 1);
                            linePos += delimiterFollowingString.length();
                            return value;
                        }
                        // not a delimiter
                    }
                    if (isEndOfLine(c)) {
                        recordState = RecordState.END;
                        return line.substring(valueStartPos, linePos);
                    }
                    else if (isSpace(c) && trimIfNotQuoted) {
                        valueEndPos = linePos - 1;  // this is possibly end of value
                        columnState = ColumnState.LAST_TRIM_OR_VALUE;

                        // TODO not implemented yet foo""bar""baz -> [foo, bar, baz].append
                        //} else if (isQuote(c)) {
                        //    // In RFC4180, If fields are not enclosed with double quotes, then
                        //    // double quotes may not appear inside the fields. But they are often
                        //    // included in the fields. We should care about them later.
                    }
                    else {
                        // keep VALUE state
                    }
                    break;

                case LAST_TRIM_OR_VALUE:
                    if (isDelimiter(c)) {
                        if (delimiterFollowingString == null) {
                            return line.substring(valueStartPos, valueEndPos);
                        }
                        else if (isDelimiterFollowingFrom(linePos)) {
                            linePos += delimiterFollowingString.length();
                            return line.substring(valueStartPos, valueEndPos);
                        }
                        else {
                            // not a delimiter
                        }
                    }
                    if (isEndOfLine(c)) {
                        recordState = RecordState.END;
                        return line.substring(valueStartPos, valueEndPos);
                    }
                    else if (isSpace(c)) {
                        // keep LAST_TRIM_OR_VALUE state
                    } else {
                        // this spaces are not trailing spaces. go back to VALUE state
                        columnState = ColumnState.VALUE;
                    }
                    break;

                case QUOTED_VALUE:
                    if (isEndOfLine(c)) {
                        // multi-line quoted value
                        quotedValue.append(line.substring(valueStartPos, linePos));
                        quotedValue.append(newline);
                        quotedValueLines.add(line);
                        if (!nextLine(false)) {
                            throw new InvalidValueException("Unexpected end of line during parsing a quoted value");
                        }
                        valueStartPos = 0;
                    }
                    else if (isQuote(c)) {
                        char next = peekNextChar();
                        if (isQuote(next)) { // escaped quote
                            quotedValue.append(line.substring(valueStartPos, linePos));
                            valueStartPos = ++linePos;
                        }
                        else {
                            quotedValue.append(line.substring(valueStartPos, linePos - 1));
                            columnState = ColumnState.AFTER_QUOTED_VALUE;
                        }
                    }
                    else if (isEscape(c)) {  // isQuote must be checked first in case of quote == escape
                        // In RFC 4180, CSV's escape char is '\"'. But '\\' is often used.
                        char next = peekNextChar();
                        if (isEndOfLine(c)) {
                            // escape end of line. TODO assuming multi-line quoted value without newline?
                            quotedValue.append(line.substring(valueStartPos, linePos));
                            quotedValueLines.add(line);
                            if (!nextLine(false)) {
                                throw new InvalidValueException("Unexpected end of line during parsing a quoted value");
                            }
                            valueStartPos = 0;
                        }
                        else if (isQuote(next) || isEscape(next)) { // escaped quote
                            quotedValue.append(line.substring(valueStartPos, linePos - 1));
                            quotedValue.append(next);
                            valueStartPos = ++linePos;
                        }
                    }
                    else {
                        if ((linePos - valueStartPos) + quotedValue.length() > maxQuotedSizeLimit) {
                            throw new QuotedSizeLimitExceededException("The size of the quoted value exceeds the limit size (" + maxQuotedSizeLimit + ")");
                        }
                        // keep QUOTED_VALUE state
                    }
                    break;

                case AFTER_QUOTED_VALUE:
                    if (isDelimiter(c)) {
                        if (delimiterFollowingString == null) {
                            return quotedValue.toString();
                        }
                        else if (isDelimiterFollowingFrom(linePos)) {
                            linePos += delimiterFollowingString.length();
                            return quotedValue.toString();
                        }
                        // not a delimiter
                    }
                    if (isEndOfLine(c)) {
                        recordState = RecordState.END;
                        return quotedValue.toString();
                    }
                    else if (isSpace(c)) {
                        // column has trailing spaces and quoted. TODO should this be rejected?
                    } else {
                        columnState = ColumnState.QUOTED_VALUE;
                    }
                    break;

                default:
                    assert false;
            }
        }
    }

    public String nextColumnOrNull()
    {
        String v = nextColumn();
        if (nullStringOrNull == null) {
            if (v.isEmpty()) {
                if (wasQuotedColumn) {
                    return "";
                }
                else {
                    return null;
                }
            }
            else {
                return v;
            }
        }
        else {
            if (v.equals(nullStringOrNull)) {
                return null;
            }
            else {
                return v;
            }
        }
    }

    public boolean wasQuotedColumn()
    {
        return wasQuotedColumn;
    }

    private char nextChar()
    {
        Preconditions.checkState(line != null, "nextColumn is called after end of file");

        if (linePos >= line.length()) {
            return END_OF_LINE;
        }
        else {
            return line.charAt(linePos++);
        }
    }

    private char peekNextChar()
    {
        Preconditions.checkState(line != null, "peekNextChar is called after end of file");

        if (linePos >= line.length()) {
            return END_OF_LINE;
        }
        else {
            return line.charAt(linePos);
        }
    }

    private boolean isSpace(char c)
    {
        return c == ' ';
    }

    private boolean isDelimiterFollowingFrom(int pos)
    {
        if (line.length() < pos + delimiterFollowingString.length()) {
            return false;
        }
        for (int i = 0; i < delimiterFollowingString.length(); i++) {
            if (delimiterFollowingString.charAt(i) != line.charAt(pos + i)) {
                return false;
            }
        }
        return true;
    }

    private boolean isDelimiter(char c)
    {
        return c == delimiterChar;
    }

    private boolean isEndOfLine(char c)
    {
        return c == END_OF_LINE;
    }

    private boolean isQuote(char c)
    {
        return quote != NO_QUOTE && c == quote;
    }

    private boolean isEscape(char c)
    {
        return escape != NO_ESCAPE && c == escape;
    }

    public static class InvalidFormatException
            extends DataException
    {
        public InvalidFormatException(String message)
        {
            super(message);
        }
    }

    public static class InvalidValueException
            extends DataException
    {
        public InvalidValueException(String message)
        {
            super(message);
        }
    }

    public static class QuotedSizeLimitExceededException
            extends InvalidValueException
    {
        public QuotedSizeLimitExceededException(String message)
        {
            super(message);
        }
    }

    public class TooManyColumnsException
            extends InvalidFormatException
    {
        public TooManyColumnsException(String message)
        {
            super(message);
        }
    }

    public class TooFewColumnsException
            extends InvalidFormatException
    {
        public TooFewColumnsException(String message)
        {
            super(message);
        }
    }

    public static class QuoteCharacter
    {
        private final char character;

        public QuoteCharacter(char character)
        {
            this.character = character;
        }

        public static QuoteCharacter noQuote()
        {
            return new QuoteCharacter(CsvTokenizer.NO_QUOTE);
        }

        @JsonCreator
        public static QuoteCharacter ofString(String str)
        {
            if (str.length() >= 2) {
                throw new ConfigException("\"quote\" option accepts only 1 character.");
            }
            else if (str.isEmpty()) {
                LoggerFactory.getLogger(CsvTokenizer.class).warn("Setting '' (empty string) to \"quote\" option is obsoleted. Currently it becomes '\"' automatically but this behavior will be removed. Please set '\"' explicitly.");
                return new QuoteCharacter('"');
            }
            else {
                return new QuoteCharacter(str.charAt(0));
            }
        }

        @JsonIgnore
        public char getCharacter()
        {
            return character;
        }

        @JsonValue
        public String getOptionalString()
        {
            return new String(new char[] { character });
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + character;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof QuoteCharacter)) {
                return false;
            }
            QuoteCharacter o = (QuoteCharacter) obj;
            return character == o.character;
        }
    }

    public static class EscapeCharacter
    {
        private final char character;

        public EscapeCharacter(char character)
        {
            this.character = character;
        }

        public static EscapeCharacter noEscape()
        {
            return new EscapeCharacter(CsvTokenizer.NO_ESCAPE);
        }

        @JsonCreator
        public static EscapeCharacter ofString(String str)
        {
            if (str.length() >= 2) {
                throw new ConfigException("\"escape\" option accepts only 1 character.");
            }
            else if (str.isEmpty()) {
                LoggerFactory.getLogger(CsvTokenizer.class).warn("Setting '' (empty string) to \"escape\" option is obsoleted. Currently it becomes null automatically but this behavior will be removed. Please set \"escape: null\" explicitly.");
                return noEscape();
            }
            else {
                return new EscapeCharacter(str.charAt(0));
            }
        }

        @JsonIgnore
        public char getCharacter()
        {
            return character;
        }

        @JsonValue
        public String getOptionalString()
        {
            return new String(new char[] { character });
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof EscapeCharacter)) {
                return false;
            }
            EscapeCharacter o = (EscapeCharacter) obj;
            return character == o.character;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + character;
            return result;
        }
    }
}
