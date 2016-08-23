#! /bin/bash

EXCHANGE_HOST=localhost
EXCHANGE_RECV=8081
EXCHANGE_SEND=8080
NUM_NODES=2
STACK_SIZE=32
NUM_CPUS=1
HANDLER_COUNT=1
LEVEL=3
TEST_TO_RUN=ckt_sim
PIECES=4
TASKLEVEL=3
EVENTLEVEL=2

# Kill all background jobs when this script exits
trap 'kill $(jobs -p)' EXIT

echo "Starting REALM tests..."
../../fabric_startup_server/exchange $NUM_NODES &
echo "Exchange started... "

# This is the 'head' runtime, output is always visible
./$TEST_TO_RUN -p $PIECES -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:cpu $NUM_CPUS -level task=$TASKLEVEL,event=$EVENTLEVEL,$LEVEL &

# Wait a bit so head runtime will actually get ID 0, without being swiped by another node.
sleep 1

# Other rt's will do the same thing as the root. Pipe their output to /dev/null to suppress it
for ((c=1; c<$NUM_NODES; c++))
do
    echo "Starting listener" $c
    ./$TEST_TO_RUN -p $PIECES -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:cpu $NUM_CPUS -level task=$TASKLEVEL,event=$EVENTLEVEL,$LEVEL & 
done

echo "All runtimes launched... "

wait
