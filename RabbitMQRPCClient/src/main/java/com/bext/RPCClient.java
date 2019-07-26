package com.bext;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RPCClient implements AutoCloseable {
	private Connection connection;
	private Channel channel;
    private String requestQueueName = "rpc_queue";
	
    public RPCClient() throws IOException, TimeoutException {
       ConnectionFactory factory = new ConnectionFactory();
       factory.setHost("localhost");
       
       connection = factory.newConnection();
       channel = connection.createChannel();
    }
    
	public static void main(String[] args) {
		long startTime = System.nanoTime();
		
		try( RPCClient fibonacciRpc = new RPCClient()) {
			for (int i = 0 ; i < 32; i++ ) {
				String i_str = Integer.toString(i);
				System.out.println("[x] Peticion de fib(" + i_str + ")");
				String response = fibonacciRpc.call(i_str);
				System.out.println("[.] Se obtiene '" + response + "'");
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
		long endTime = System.nanoTime();
		System.out.println("tiempo de ejecución de proceso: "+ (endTime - startTime) + " nanoSeg.");
	}

	private String call(String message) throws IOException, InterruptedException {
		final String corrId = UUID.randomUUID().toString();
		
		String replyQueueName = channel.queueDeclare().getQueue();
		AMQP.BasicProperties props = new AMQP.BasicProperties()
				.builder()
				.correlationId(corrId)
				.replyTo(replyQueueName)
				.build();
		String exchange = "";
		String routingKey = requestQueueName;
		channel.basicPublish(exchange, routingKey, props, message.getBytes("UTF-8"));
		
		final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);
		boolean autoAck = true; 
		System.out.println("-> channel.basicConsume [queue:"+replyQueueName+", corrId:"+corrId+"]...");
		String ctag = channel.basicConsume( replyQueueName, autoAck,
				(consumerTag, delivery)-> {
					if (delivery.getProperties().getCorrelationId().equals(corrId)) {
						response.offer(new String(delivery.getBody(),"UTF-8"));
					}
				}, consumerTag->{}
				);
		System.out.println("... channel.basicConsume ->");
		String result = response.take();
		System.out.println("... response.take() ->");
		channel.basicCancel(ctag);
		System.out.println("... basicCancel(ctag) ->");
		return result;
	}

	@Override
	public void close() throws IOException {
		connection.close();
	}
}
