#!/bin/sh

wget -nc -b ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/*.gz -P baseline
wget -nc -b ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/*.gz -P updates