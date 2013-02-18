<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">
    <html>
    <head>
        <script src="sorttable.js"></script>
        <style type="text/css">
            h1 {
                font-size: 16px;
                font-weight: bold;
                line-height: 18px;
                margin: 10px 0;
                }
            h1.page {
                     page-break-before: always;
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
                   }
            td {border-top: 1px solid #DEDEDE;}
            td.number {
                       text-align: right;
                       type: 'xs:decimal';
                       font-family: Courier;
                       }
            tfoot {
                   background-color: #DEDEDE;
                   font-weight: bold; 
                   }
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

<xsl:template match='report'>
    <h1><xsl:value-of select="description" /> Overview</h1>
    <span class='date'>
        as of <xsl:apply-templates select='date'/>
    </span>
    <table class='sortable'>
        <thead>
            <tr>
                <th class='category'>Asset Type</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
                <th class='number'>Expense Ratio</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Cash</td>
                <xsl:apply-templates select="dollar_value[@type='cash']"/>
                <xsl:apply-templates select="proportion[@type='cash']"/>
            </tr>
            <tr>
                <td>Equities</td>
                <xsl:apply-templates select="securities[@type='Equities']/dollar_value[@type='total']"/>
                <xsl:apply-templates select="securities[@type='Equities']/proportion[@type='total']"/>
            </tr>
            <tr>
                <td>Options</td>
                <xsl:apply-templates select="securities[@type='Options']/dollar_value[@type='total']"/>
                <xsl:apply-templates select="securities[@type='Options']/proportion[@type='total']"/>
            </tr>
            <tr>
                <td>ETFs</td>
                <xsl:apply-templates select="funds[@type='ETFs']/dollar_value"/>
                <xsl:apply-templates select="funds[@type='ETFs']/proportion"/>
                <xsl:apply-templates select="funds[@type='ETFs']/expense_ratio"/>
            </tr>
            <tr>
                <td>Mutual Funds</td>
                <xsl:apply-templates select="funds[@type='Mutual Funds']/dollar_value"/>
                <xsl:apply-templates select="funds[@type='Mutual Funds']/proportion"/>
                <xsl:apply-templates select="funds[@type='Mutual Funds']/expense_ratio"/>
            </tr>
        </tbody>
        <tfoot>
            <tr>
                <td>Total</td>
                <xsl:apply-templates select="dollar_value[@type='overall']"/>
                <xsl:apply-templates select="proportion[@type='overall']"/>
                <xsl:apply-templates select="expense_ratio" />
            </tr>
        </tfoot>
    </table>
    <xsl:apply-templates select="securities[@type='Equities']"/>
    <xsl:apply-templates select="securities[@type='Options']"/>
    <xsl:apply-templates select="funds[@type='ETFs']"/>
    <xsl:apply-templates select="funds[@type='Mutual Funds']"/>
    <xsl:apply-templates select='allocation_reports'/>
    <xsl:apply-templates select='aggregated_holdings'/>
</xsl:template>

<xsl:template match='allocation_reports'>
        <xsl:apply-templates select='allocation_report'/>
</xsl:template>

<xsl:template match='funds'>
    <h1 class='page'><xsl:value-of select="@type"/> Held</h1>
    <table class='sortable'>
        <thead>
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
            <xsl:apply-templates select='fund'>
                <xsl:sort select="ticker"/>
            </xsl:apply-templates>
        </tbody>
        <tfoot>
            <tr>
                <td>Total</td>
                <td></td>
                <td></td>
                <xsl:apply-templates select="expense_ratio"/>
                <td></td>
                <xsl:apply-templates select="dollar_value"/>
                <xsl:apply-templates select="proportion"/>
            </tr>
        </tfoot>
    </table>
</xsl:template>

<xsl:template match='securities'>
    <h1 class='page'><xsl:value-of select="@type"/> Held</h1>
    <table class='sortable'>
        <thead>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='number'>Quantity</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='security'>
                <xsl:sort select='ticker'/>
            </xsl:apply-templates>
        </tbody>
        <tfoot>
            <tr>
                <td>Total Long</td>
                <td></td>
                <td></td>
                <xsl:apply-templates select="dollar_value[@type='long']"/>
                <xsl:apply-templates select="proportion[@type='long']"/>
            </tr>
            <tr>
                <td>Total Short</td>
                <td></td>
                <td></td>
                <xsl:apply-templates select="dollar_value[@type='short']"/>
                <xsl:apply-templates select="proportion[@type='short']"/>
            </tr>
            <tr>
                <td>Total</td>
                <td></td>
                <td></td>
                <xsl:apply-templates select="dollar_value[@type='total']"/>
                <xsl:apply-templates select="proportion[@type='total']"/>
            </tr>
        </tfoot>
    </table>
</xsl:template>

<xsl:template match='allocation_report'>
    <h1 class='page'><xsl:value-of select="@title"/></h1>
    <table class='sortable'>
        <thead>
            <tr>
                <th class='category'>Category</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='category'/>
        </tbody>
        <tfoot>
            <tr>
                <td>Total</td>
                <xsl:apply-templates select="dollar_value"/>
                <xsl:apply-templates select="proportion"/>
            </tr>
        </tfoot>
    </table>
</xsl:template>

<xsl:template match='aggregated_holdings'>
    <h1 class='page'>Top Aggregated Holdings</h1>
    <table class='sortable'>
        <thead>
            <tr>
                <th class='category'>Symbol</th>
                <th class='description'>Description</th>
                <th class='description'>Held By/Value</th>
                <th class='number'>Value</th>
                <th class='number'>Proportion</th>
            </tr>
        </thead>
        <tbody>
            <xsl:apply-templates select='holding'>
                <xsl:sort select="proportion" order='descending'/>
            </xsl:apply-templates>
        </tbody>
        <tfoot>
            <tr>
                <td>Total</td>
                <td></td>
                <td></td>
                <xsl:apply-templates select="dollar_value"/>
                <xsl:apply-templates select="proportion"/>
            </tr>
        </tfoot>
    </table>
</xsl:template>

<xsl:template match='holding'>
    <tr>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='description'/>
        <td>
            <ul>
                <xsl:apply-templates select='held_by'/>
            </ul>
        </td>
        <xsl:apply-templates select='dollar_value'/>
        <xsl:apply-templates select='proportion'/>
    </tr>
</xsl:template>

<xsl:template match='held_by'>
    <li>
        <xsl:apply-templates select='ticker'/>
        <xsl:apply-templates select='dollar_value'/>
    </li>
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

<xsl:template match='security'>
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
    <td class='number'>
        <xsl:value-of select="format-number(., '$#,##0.00')" />
    </td>
</xsl:template>

<xsl:template match='expense_ratio'>
    <td class='number'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

<xsl:template match='proportion'>
    <td class='number'>
        <xsl:value-of select="format-number(., '0.00%')" />
    </td>
</xsl:template>

<xsl:template match='quantity'>
    <td class='number'>
        <xsl:value-of select="format-number(., '#,##0.000')" />
    </td>
</xsl:template>

</xsl:stylesheet>
