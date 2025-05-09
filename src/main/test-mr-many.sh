#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x test-mr.sh

# 检查是否有timeout命令
if command -v timeout &> /dev/null; then
    HAS_TIMEOUT=true
else
    HAS_TIMEOUT=false
    echo "Warning: 'timeout' command not found, running without timeouts"
fi

for i in $(seq 1 $runs); do
    echo "*** Starting test trial $i of $runs"
    if [ "$HAS_TIMEOUT" = true ]; then
        timeout -k 2s 900s ./test-mr.sh &
    else
        ./test-mr.sh &
    fi
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
    echo "*** Completed test trial $i successfully"
done
echo '***' PASSED ALL $i TESTING TRIALS
