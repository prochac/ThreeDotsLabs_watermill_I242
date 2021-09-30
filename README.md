Reproduction of https://github.com/ThreeDotsLabs/watermill/issues/242

Run RabbitMQ with included config. Even that I use 5000ms as ACK timeout, the ack times out after one minute.

Then run the Go program. It creates a queue and publishes 3 messages. The first one is only for verification that everything works. The second message triggers the timeout. The third is for demonstration that the consumer got stuck. 
