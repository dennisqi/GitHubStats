#!/bin/bash

while read p; do
    gz_file=${p##*/}
    file_name=${gz_file::${#gz_file} - 3}
    echo $gz_file
    bucket_name=gharchive
    url=http://data.$bucket_name.org/$file_name.gz
    wget -qO- $p | gunzip | aws s3 cp - s3://$bucket_name/$file_name | echo $url >> $saved_urls
done < $1

rm $coming_urls | touch $coming_urls

# ../../data/coming_urls.txt
# /home/ubuntu/GitHubStats/data/saved_urls.txt
