#!/bin/bash

exec python ./scrapDeckLinks.py &
exec python ./deckDownloader.py &
exec python ./idFetcher.py