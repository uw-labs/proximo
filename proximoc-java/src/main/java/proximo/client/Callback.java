package proximo.client;
public interface Callback {
	void OnMessage(byte[] message) throws MessageException;
}
