#!/bin/bash

curl --header "Content-Type: application/json" -X POST \
     --data '{"source_id": "video", "from": 1715515849, "to": null, "limit": 100}' \
     http://127.0.0.1:8080/api/v1/keyframes/find