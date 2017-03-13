package proximo.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import proximo.MessageSourceGrpc;
import proximo.Proximo;
import proximo.Proximo.ConsumerRequest;
import proximo.Proximo.Message;

public class Consumer {
	public static void consume(String host, int port, String topic, String consumerId, Callback callback) {

		BlockingQueue<Message> messages = new SynchronousQueue<Message>();

		CountDownLatch latch = new CountDownLatch(1);

		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
		MessageSourceGrpc.MessageSourceStub stub = MessageSourceGrpc.newStub(channel);

		io.grpc.stub.StreamObserver<ConsumerRequest> cr = stub.consume(new io.grpc.stub.StreamObserver<Message>() {

			@Override
			public void onCompleted() {
				channel.shutdown();
				latch.countDown();
			}

			@Override
			public void onError(Throwable msg) {
				channel.shutdownNow();
				latch.countDown();
			}

			@Override
			public void onNext(Message msg) {
				try {
					messages.put(msg);
				} catch (InterruptedException e) {
					channel.shutdown();
					latch.countDown();
				}
			}

		});

		Proximo.StartConsumeRequest scr = Proximo.StartConsumeRequest.newBuilder().setTopic(topic)
				.setConsumer(consumerId).build();
		cr.onNext(Proximo.ConsumerRequest.newBuilder().setStartRequest(scr).build());

		for (;;) {
			try {
				Message msg = messages.take();
				callback.OnMessage(msg.getData().toByteArray());
				Proximo.Confirmation conf = Proximo.Confirmation.newBuilder().setMsgID(msg.getId()).build();
				cr.onNext(Proximo.ConsumerRequest.newBuilder().setConfirmation(conf).build());
			} catch (MessageException | InterruptedException e) {
				channel.shutdown();
				break;

			}
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
		}
	}
}
