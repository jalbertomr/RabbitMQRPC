package com.bext;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RPCServer {
	private static final String RPC_QUEUE_NAME = "rpc_queue";

	public static void main(String[] args) throws IOException, TimeoutException {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try (Connection connection = factory.newConnection();
			 Channel channel = connection.createChannel();	) {
			boolean durable = false;
			boolean exclusive = false;
			boolean autoDelete = false;
			channel.queueDeclare(RPC_QUEUE_NAME, durable, exclusive, autoDelete, null /*arguments*/);
			channel.queuePurge(RPC_QUEUE_NAME);
			
			channel.basicQos(1);
			
			System.out.println("[x] Esperando peticiones RPC...");
			
			Object monitor = new Object();
			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			   AMQP.BasicProperties replyProps = new AMQP.BasicProperties
					   .Builder()
					   .correlationId(delivery.getProperties().getCorrelationId())
					   .build();
			   
			   String response = "";
			   
			   try {
				  String message = new String(delivery.getBody(), "UTF-8");
				  int n = Integer.parseInt(message);
				  System.out.println("[.] fib(" + message + ")");
				  response += fib(n);
			   } catch (RuntimeException e) {
				  System.out.println("[.] " + e.toString());
			   } finally {
				  String exchange = ""; 
				  String routingKey = delivery.getProperties().getReplyTo();
				  channel.basicPublish(exchange, routingKey, replyProps, response.getBytes("UTF-8"));
				  System.out.println(("-> channel.basicPublish [replyTo:"+routingKey+", corrId:"+delivery.getProperties().getCorrelationId()+"]..."));
				  boolean multiple = false;
				  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), multiple);
				// RabbitMq consumer worker thread notifies the RPC server owner thread
				  synchronized(monitor) {
					  monitor.notify();
				  }
			   }
			};
			boolean autoAck = false;
			channel.basicConsume(RPC_QUEUE_NAME, autoAck, deliverCallback, consumerTag->{} );
			
			// Wait and be prepared to consume the message from RPC client.
			while(true) {
				synchronized(monitor) {
					try {
						monitor.wait();
					} catch (InterruptedException e2) {
						e2.printStackTrace();
					}
				}
			}
		}
	}

	private static int fib(int n) {
		if (n == 0) return 0;
		if (n == 1) return 1;
		return fib(n - 1) + fib(n - 2);		
	}
}
