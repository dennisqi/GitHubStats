#!/bin/bash

while read p; do
    gz_file=${p##*/}
    file_name=${gz_file::${#gz_file} - 3}
    echo $gz_file
    bucket=gharchive-compressed
    url=http://data.$bucket.org/$file_name.gz
    wget -qO- $p | gunzip | aws s3 cp - s3://$bucket/$file_name | rm $3
    echo $url >> $3 | echo $url >> $2
done <$1

# ../../data/coming_urls.txt
# /home/ubuntu/GitHubStats/data/saved_urls.txt
# /home/ubuntu/GitHubStats/data/latest_url.txt
