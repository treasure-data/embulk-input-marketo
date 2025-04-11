package org.embulk.input.marketo;

import org.embulk.config.ConfigException;
import org.embulk.util.text.LineDecoder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class CsvTokenizerTest
{
    @Test
    public void testQuotesInQuotedFields()
    {
        Assert.assertEquals(CsvTokenizer.QuotesInQuotedFields.NONE, CsvTokenizer.QuotesInQuotedFields.from("  nOne  "));
        Assert.assertEquals(CsvTokenizer.QuotesInQuotedFields.ACCEPT_ONLY_RFC4180_ESCAPED, CsvTokenizer.QuotesInQuotedFields.from("  aCCept_only_rfc4180_escaped  "));
        Assert.assertEquals(CsvTokenizer.QuotesInQuotedFields.ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS, CsvTokenizer.QuotesInQuotedFields.from("  AcCePt_stRaY_qUoTeS_aSsUmInG_nO_dElImItErS_iN_fIeLdS  "));
        try {
            CsvTokenizer.QuotesInQuotedFields.from("iNvAlId");
        }
        catch (Exception e) {
            Assert.assertTrue(e instanceof ConfigException);
            Assert.assertEquals("Unsupported quotes_in_quoted_fields: 'iNvAlId', supported values: [NONE, ACCEPT_ONLY_RFC4180_ESCAPED, ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS]", e.getMessage());
        }
    }

    @Test
    public void testInitCsvTokenizer()
    {
        try {
            LineDecoder lineDecoder = Mockito.mock(LineDecoder.class);
            new CsvTokenizer(",", '"', '"', "\n", true,
                    CsvTokenizer.QuotesInQuotedFields.ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS, 1, "", lineDecoder, "null");
            Assert.fail("Expected Exception to be thrown");
        }
        catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertEquals("[quotes_in_quoted_fields == ACCEPT_STRAY_QUOTES_ASSUMING_NO_DELIMITERS_IN_FIELDS] is not allowed to specify with [trim_if_not_quoted = true]", e.getMessage());
        }
    }
}
