<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
    <html>
    <head>
    <style type="text/css">
        h1 {
            font-size: 16px;
            font-weight: bold;
            line-height: 18px;
            margin: 10px 0;
            }
        span.date {
                   position: absolute;
                   right: 20px;
                   top: 18px;
                   font-size: 10px;
                   }
        table {
               border-bottom: 1px solid #BFBFBF;
               border-top: 1px solid #BFBFBF;
               width: 100%;
               font-size: 12px;
               border-collapse: collapse;
               border-spacing: 0;
               margin-bottom: 30px;
               position: relative;
               }
        td {border-top: 1px solid #DEDEDE;}
        th {
            background-color: #EAEAEA;
            border-bottom: 1px solid #BFBFBF;
            font-size: 12px;
            padding: 7px 10px 7px 0;
            vertical-align: bottom;
            white-space: nowrap;
            }
        th.number {text-align: right}
        th.category {text-align: left}
        th.description {text-align: left}
    </style>
     <title>
            Portfolio Allocation as of <xsl:apply-templates select='report/date'/>
        </title>
    </head>
    <body>
    <xsl:apply-templates/>
    </body>
    </html>
</xsl:template>

x<xsl:template match='report'>
    <table>
        <thead>
            <h1>Portfolio Overview</h1>
            <span class='date'>
                as of <xsl:apply-templates select='date'/>
            </span>
            <tr>
                <th class='category'>Asset Type</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
                <th class='number'>Expense Ratio</th>
            </tr>
        </thead>
        <tbody>
            <tr style='color:blue'>
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
    <table>
        <thead>
            <h1>Equities Held</h1>
            <span class='date'>
                as of <xsl:apply-templates select='/report/date'/>
            </span>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='number'>Quantity</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='equity'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='etfs'>
    <table>
        <thead>
            <h1>ETFs Held</h1>
            <span class='date'>
                as of <xsl:apply-templates select='/report/date'/>
            </span>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='number'>Gross Expense</th>
                <th class='number'>Net Expense</th>
                <th class='number'>Quantity</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='fund'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='mfs'>
    <table>
        <thead>
            <h1>Mutual Funds Held</h1>
            <span class='date'>
                as of <xsl:apply-templates select='/report/date'/>
            </span>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='number'>Gross Expense</th>
                <th class='number'>Net Expense</th>
                <th class='number'>Quantity</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='fund'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='options'>
    <table>
        <thead>
            <h1>Options Held</h1>
            <span class='date'>
                as of <xsl:apply-templates select='/report/date'/>
            </span>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='number'>Quantity</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='option'/>
        </tbody>
    </table>
</xsl:template>

<xsl:template match='allocation_report'>
    <table>
        <thead>
            <h1><xsl:value-of select="@title"/></h1>
            <span class='date'>
                as of <xsl:apply-templates select='/report/date'/>
            </span>
            <tr>
                <th class='category'>Category</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
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
        <xsl:apply-templates select='quantity'/>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='option'>
    <tr>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='description'/>
        <xsl:apply-templates select='quantity'/>
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
        <xsl:apply-templates select='quantity'/>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='date'>
    <xsl:value-of select="."/>
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

<xsl:template match='dollar_value'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '$#,##0.00')" />
    </td>
</xsl:template>

<xsl:template match='expense_ratio'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

<xsl:template match='proportion'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

<xsl:template match='quantity'>
    <td align='right' type='xs:decimal'>
        <xsl:value-of select="format-number(., '#,##0.000')" />
    </td>
</xsl:template>

</xsl:stylesheet>
