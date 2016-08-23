#! /bin/bash

EXCHANGE_HOST=localhost
EXCHANGE_RECV=8081
EXCHANGE_SEND=8080
NUM_NODES=2
STACK_SIZE=1
NUM_CPUS=4
HANDLER_COUNT=3
TEST_TO_RUN=ghost

# Kill all background jobs when this script exits
trap 'kill $(jobs -p)' EXIT

echo "Starting REALM tests..."
../../fabric_startup_server/exchange $NUM_NODES &
echo "Exchange started... "

# This is the 'head' runtime, output is always visible
./$TEST_TO_RUN -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:cpu $NUM_CPUS -level log_annc=0 &

# Wait a bit so head runtime will actually get ID 0, without being swiped by another node.
sleep 1

# Other rt's will do the same thing as the root. Pipe their output to /dev/null to suppress it
for ((c=1; c<$NUM_NODES; c++))
do
    ./$TEST_TO_RUN -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:cpu $NUM_CPUS -level log_annc=0 & 
done

echo "All runtimes launched... "

wait
