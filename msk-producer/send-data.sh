#!/bin/bash

input_file="movies.txt"

url="http://localhost:8080/"

while IFS= read -r line; do
  curl -X POST -d "$line" "$url"
  #sleep 1s
done < "$input_file"
