#!/usr/bin/env bash

# Main "driver" script. Tasks:
#
#   1. Central place for defining global configuration variables and utility
#      functions.
#   2. Harvest metadata records.
#   3. Analyze harvested metadata records.

# Configuration environment variables
export BASE_DIR=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)
export SAXON_DIR="${SAXON_DIR:-/data/bedi-web/saxon}"
export JAVA="/usr/bin/java"
export COLLECTIONS_DIR="${COLLECTIONS_DIR:-/data/bedi}"
export RESULTS_DIR="${RESULTS_DIR:-/data/bedi}"
export EVAL_XSL_DIR="${EVAL_XSL_DIR:-$BASE_DIR/bedi-site/bediTools/xsl}"
export DATESTAMP=${DATESTAMP:-$(date +%Y%m%d)}
export PYTHON3=${PYTHON3:-/usr/bin/python3}

# Provide three different lists of metadata providers. The output can be
# readily consumed in for/while loops. The sources of providers are two text
# files: $BASE_DIR/bin/nasa_providers.txt and
# $BASE_DIR/bin/other_providers.txt.
#
# 1. Argument "nasa": The list of NASA data centers
# 2. Argument "other": Other providers
# 3. Without an argument: The lists above combined as one
function providers {
    if [[ "$1" != "other" ]]; then
        while read P; do
            echo $P
        done < $BASE_DIR/bin/nasa_providers.txt
    fi

    if [[ "$1" != "nasa" ]]; then
        while read P; do
            echo $P
        done < $BASE_DIR/bin/other_providers.txt
    fi
}
export -f providers

#
# Get collections of metadata records
#
for provider in $(providers "all"); do
    dpath="$COLLECTIONS_DIR/collections/NASA_CMR/${provider}__${DATESTAMP}"
    mkdir -p -v $dpath
    curl "https://cmr.earthdata.nasa.gov/search/collections"`
         `"?page-size=2000&provider=$provider" \
         > $dpath/collections.xml
done

#
# Get metadata records
#

# Create a temporary directory
TDIR=$(mktemp -d)

# Loop through providers to create curl commands
for provider in $(providers "all"); do
    curl_file="$TDIR/${provider}_curl"
    rec_dir="${COLLECTIONS_DIR}/collections/NASA_CMR/${provider}__${DATESTAMP}/ISO/xml"
    mkdir -p -v $rec_dir
    $JAVA -jar $SAXON_DIR/saxon9he.jar \
        -s:"${COLLECTIONS_DIR}/collections/NASA_CMR/${provider}__${DATESTAMP}/collections.xml" \
        -xsl:${BASE_DIR}/xsl/makeCurlLinks.xsl \
        toDir=$rec_dir \
        > $curl_file

    echo "Download metadata records for $provider from $curl_file"
    while read -r url || [[ -n "$url" ]]; do
        to_file=$(echo $url | cut -d "|" -f2)
        url=$(echo $url | cut -d "|" -f1)
        curl -o $to_file $url
        curl_status=$?
        if test "$curl_status" != "0"; then
            rm -f $to_file
            curl $url
            curl_status=$?
            if test "$curl_status" != "0"; then
                rm -f $to_file
               echo "ERROR: Download of '$to_file' failed." 1>&2
           fi
        fi
    done < $curl_file
done

echo "Removing provider curl files"
rm -rvf $TDIR

#
# Evaluate metadata records
#

# Create a temporary directory
TDIR=$(mktemp -d)

for provider in $(providers "all"); do
    records_dir="$COLLECTIONS_DIR/collections/NASA_CMR/"`
                `"${provider}__${DATESTAMP}/ISO/xml"

    $JAVA -Xdiag -jar ${SAXON_DIR}/saxon9.jar \
        -s:$BASE_DIR/bin/dummy.xml \
        -xsl:${EVAL_XSL_DIR}/KnownNodes.xsl \
        fileNamePattern=*.xml \
        recordSetPath=$records_dir \
        >  $TDIR/${provider}_known.csv

    $JAVA -Xdiag -jar ${SAXON_DIR}/saxon9.jar \
        -s:$BASE_DIR/bin/dummy.xml \
        -xsl:${EVAL_XSL_DIR}/AllNodes.xsl \
        fileNamePattern=*.xml \
        recordSetPath=$records_dir \
        >  $TDIR/${provider}_all.csv

    mkdir -p -v $RESULTS_DIR/data/NASA_CMR

    $PYTHON3 <<CODE
import pandas as pd

df_known = pd.read_csv('$TDIR/${provider}_known.csv')
df_all = pd.read_csv('$TDIR/${provider}_all.csv')
df_unknown = df_all[(~df_all.XPath.isin(df_known.XPath))]
df = pd.concat(
    [df_known, df_unknown], axis=0).sort_values(['Record', 'Concept'])
df.drop_duplicates(inplace=True)
df.fillna('Unknown', inplace=True)
df.to_csv('$RESULTS_DIR/data/NASA_CMR/${provider}__${DATESTAMP}.csv.gz',
          index=False,
          compression='gzip')
CODE
done

rm -rvf $TDIR
