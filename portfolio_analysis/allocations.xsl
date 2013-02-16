<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
    <html>
    <body>
    <xsl:apply-templates/>
    </body>
    </html>
</xsl:template>

<xsl:template match='report'>
    <h1>Portfolio Overview</h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Asset Type</th>
                <th>Value</th>
                <th>Proportion</th>
                <th>Expense Ratio</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Total</td>
                <xsl:apply-templates select="dollar_value[@type='overall']"/>
                <xsl:apply-templates select="proportion[@type='overall']"/>
                <xsl:apply-templates select="expense_ratio" />
            </tr>
            <tr>
                <td>Cash</td>
                <xsl:apply-templates select="dollar_value[@type='cash']"/>
                <xsl:apply-templates select="proportion[@type='cash']"/>
            </tr>
            <tr>
                <td>Equities</td>
                <xsl:apply-templates select="equities/dollar_value"/>
                <xsl:apply-templates select="equities/proportion"/>
            </tr>
            <tr>
                <td>Options</td>
                <xsl:apply-templates select="options/dollar_value"/>
                <xsl:apply-templates select="options/proportion"/>
            </tr>
            <tr>
                <td>ETFs</td>
                <xsl:apply-templates select="etfs/dollar_value"/>
                <xsl:apply-templates select="etfs/proportion"/>
                <xsl:apply-templates select="etfs/expense_ratio"/>
            </tr>
            <tr>
                <td>Mutual Funds</td>
                <xsl:apply-templates select="mfs/dollar_value"/>
                <xsl:apply-templates select="mfs/proportion"/>
                <xsl:apply-templates select="mfs/expense_ratio"/>
            </tr>
        </tbody>
    </table>
    <xsl:apply-templates select='equities'/>
    <xsl:apply-templates select='etfs'/>
    <xsl:apply-templates select='mfs'/>
    <xsl:apply-templates select='options'/>
    <xsl:apply-templates select='allocation_reports'/>
</xsl:template>

<xsl:template match='allocation_reports'>
    <xsl:apply-templates select='allocation_report'/>
</xsl:template>

<xsl:template match='equities'>
    <h1>Equities Held</h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Symbol</th>
                <th>Description</th>
                <th>Value</th>
                <th>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='equity'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='etfs'>
    <h1>ETFs Held</h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Fund Symbol</th>
                <th>Description</th>
                <th>Gross Expense Ratio</th>
                <th>Net Expense Ratio</th>
                <th>Value</th>
                <th>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='fund'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='mfs'>
    <h1>Mutual Funds Held</h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Fund Symbol</th>
                <th>Description</th>
                <th>Gross Expense Ratio</th>
                <th>Net Expense Ratio</th>
                <th>Value</th>
                <th>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='fund'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='options'>
    <h1>Options Held</h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Symbol</th>
                <th>Description</th>
                <th>Value</th>
                <th>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='option'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='allocation_report'>
    <h1><xsl:value-of select="@title"/></h1>
    <table border='3'>
        <thead>
            <tr>
                <th>Category</th>
                <th>Value</th>
                <th>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='category'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='category'>
    <xsl:choose>
        <xsl:when test="not(../@title='Sector Allocation' 
                     and (name='Cash Equivalent' or name='Commingled Fund'))">
            <tr>
                <td><xsl:value-of select='name'/></td>
                <xsl:apply-templates select='dollar_value'/>        
                <xsl:apply-templates select='proportion'/>        
            </tr>
        </xsl:when>
    </xsl:choose>
</xsl:template>

<xsl:template match='equity'>
    <tr>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='description'/>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='option'>
    <tr>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='description'/>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='fund'>
    <tr>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='description'/>
        <xsl:apply-templates select="expense_ratio[@type='gross']"/>
        <xsl:apply-templates select="expense_ratio[@type='net']"/>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='ticker'>
    <td>
        <xsl:value-of select="."/>
    </td>
</xsl:template>

<xsl:template match='description'>
    <td>
        <xsl:value-of select="."/>
    </td>
</xsl:template>

<xsl:template match='expense_ratio'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

<xsl:template match='dollar_value'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '$#,##0.00')" />
    </td>
</xsl:template>

<xsl:template match='proportion'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

</xsl:stylesheet>
