package proximo.client.example;

import java.nio.charset.Charset;

import proximo.client.Callback;
import proximo.client.Consumer;
import proximo.client.MessageException;

public class Example {

	private static final Charset UTF8 = Charset.forName("UTF8");

	public static void main(String[] args) {
		Consumer.consume("172.17.0.1", 6868, "example-topic", "example-consumer", new Callback() {
			@Override
			public void OnMessage(byte[] message) throws MessageException {
				System.out.println(new String(message, UTF8));
			}
		});
	}
}
