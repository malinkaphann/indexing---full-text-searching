#!/bin/sh

fake2db --db postgresql --rows 1000000 --host $PG_HOST --user $PG_USER --password $PG_PASS --name $PG_DB --custom name email password address country
