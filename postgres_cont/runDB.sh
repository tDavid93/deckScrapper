#!/bin/sh
docker run --name mtgdb-container -p 5432:5432 -v /SQLdata1:/var/lib/postgresql/data -d mtgdb