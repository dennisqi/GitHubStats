#!/bin/bash

# for each line of url in url_generator_coming_urls.txt
# wget, unzip, and send to aws S3
# after send to s3, store the saved url in url_generator_saved_urls.txt

while read p; do
    gz_file=${p##*/}
    file_name=${gz_file::${#gz_file} - 3}
    echo $gz_file
    bucket_name=$3
    url=http://data.$bucket_name.org/$file_name.gz
    wget -qO- $p | gunzip | aws s3 cp - s3://$bucket_name/$file_name | echo $url >> $2
done < $1

rm $1 | touch $1

# /home/ubuntu/GitHubStats/data/url_generator_coming_urls.txt
# /home/ubuntu/GitHubStats/data/url_generator_saved_urls.txt
# gharchive
