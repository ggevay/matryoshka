#!/bin/bash

# Failure if we have "ERROR" in the log.
# However, ignore lines after "BlockManagerMaster stopped", because there are often irrelevant errors at Spark shutdown.
# For the sed part, see https://unix.stackexchange.com/questions/11305/show-all-the-file-up-to-the-match
if [ `sed '/BlockManagerMaster stopped/q' $1 |grep "ERROR" |wc -l` != 0 ]
then
  printf "\n!!!!!!!!!!! Spark program failed (We have ERROR in the log)\n"
  echo "Out file: $1"
  exit 72
fi

