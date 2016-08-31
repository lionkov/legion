#! /bin/bash

EXCHANGE_HOST=localhost
EXCHANGE_RECV=8081
EXCHANGE_SEND=8080
NUM_NODES=3
STACK_SIZE=1
HANDLER_COUNT=3
LEVEL=1
RSIZE=16

# Kill all background jobs when this script exits
trap 'kill $(jobs -p)' EXIT

echo "Starting tests..."
../../fabric_startup_server/exchange $NUM_NODES &

# start the runtime that will actually do a test. This is registered first so it will have ID 0.
./runtests y -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:rsize $RSIZE -level $LEVEL &

# Wait a bit so head runtime will actually get ID 0, without being swiped by another node.
sleep 1

# start all the passive RTS
for ((c=1; c<$NUM_NODES; c++))
do
    echo "Starting listener" $c
    ./runtests n -ll:exchange_server_host $EXCHANGE_HOST -ll:exchange_server_send_port $EXCHANGE_SEND -ll:exchange_server_recv_port $EXCHANGE_RECV -ll:num_nodes $NUM_NODES -ll:handlers $HANDLER_COUNT -ll:stacksize $STACK_SIZE -ll:rsize $RSIZE -level $LEVEL > /dev/null & 
done

echo "All runtimes launched... "

wait
