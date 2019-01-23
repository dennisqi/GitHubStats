#!/bin/bash

while read p; do
    gz_file=${p##*/}
    file_name=${gz_file::${#gz_file} - 3}
    echo $gz_file
    wget -qO- $p | gunzip | aws s3 cp - s3://gharchive-compressed/$file_name
done <../../data/updated_gharchive_urls.txt
