<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/">
    <html>
    <body>
    <xsl:for-each select='allocation_reports/allocation_report'>
        <h1><xsl:value-of select='@title' /></h1>
        <table>
            <th>Category</th>
            <th>Value</th>
            <th>Proportion</th>
            <xsl:for-each select='category'>
                <tr>
                <td>
                    <xsl:value-of select='name'/>
                </td>
                <td align='right' type='xs:decimal'>
                    <xsl:value-of select="format-number(dollar_amount, '$###,###,##0.000')" />
                </td>
                <td align='right' type='xs:decimal'>
                    <xsl:value-of select="format-number(wealth_proportion, '0.00%')"/>
                </td>
                </tr>
            </xsl:for-each>
        </table>
    </xsl:for-each>
    </body>
    </html>
</xsl:template>
</xsl:stylesheet>
