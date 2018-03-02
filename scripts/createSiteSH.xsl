<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xs" version="2.0">
    <!--<xsl:param name="recordSetPath"></xsl:param>-->
    <!--    <xsl:param name="fileNamePattern" select="'*.xml'"></xsl:param>
-->
    <xsl:param name="delimiter" select="','"/>
    <xsl:output method="text"/>
    <xsl:template match="/">
        <xsl:for-each select="/response/result/doc">
            <xsl:text>mkdir -p ../data/</xsl:text>
            <xsl:value-of select="substring(base-uri(), 60, 3)"/>
            <xsl:text>/</xsl:text>
            <xsl:value-of select='substring(./date[@name = "dateUploaded"], 0, 5)'/>
            <xsl:text> &amp;&amp;</xsl:text>
            <xsl:value-of select="'&#xA;'"/>
            <xsl:text>curl -L --retry 10 "</xsl:text>
            <xsl:value-of select="./str"/>
            <xsl:text>" > </xsl:text>
            <xsl:text>../data/</xsl:text>
            <xsl:value-of select="substring(base-uri(), 60, 3)"/>
            <xsl:text>/</xsl:text>
            <xsl:value-of select='substring(./date[@name = "dateUploaded"], 0, 5)'/>
            <xsl:text>/EML/xml</xsl:text>
            <xsl:value-of select="substring(replace(substring-after(./str, lower-case(substring(base-uri(), 60, 3))), '%2F', '.'), 2)"/>
            <xsl:text>.xml</xsl:text><xsl:if test="not(position()=last())"><xsl:text> &amp;&amp;</xsl:text></xsl:if>
            <!--            <xsl:value-of select="substring(base-uri(),60,3)"/>-->
            <!-- <xsl:value-of select="$delimiter"/>
            <xsl:value-of select='substring(./date[@name = "dateUploaded"],0,5)'/>
            <xsl:value-of select="$delimiter"/>
            <xsl:value-of select="./str"/>
            <xsl:value-of select="$delimiter"/>
            <xsl:value-of select="substring(replace(substring-after(./str,lower-case(substring(base-uri(),60,3))),'%2F','.'),2)"/>-->
            <xsl:value-of select="'&#xA;'"/>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>
