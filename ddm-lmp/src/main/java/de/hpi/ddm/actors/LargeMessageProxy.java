package de.hpi.ddm.actors;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Array;
import scala.collection.mutable.HashMap;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////
	// Message Buffer //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	private class LargeMessagePackageCollection {
		public int packageAmount;
		public List<LargeMessagePackage> largeMessagePackages;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	private class LargeMessagePackage {
		public int packageId;
		public byte[] data;
	}

	private HashMap<Integer, LargeMessagePackageCollection> largeMessageBuffer = new HashMap<>();

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] bytes = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(message.getMessage());
			out.flush();
			bytes = bos.toByteArray();
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		if (bytes == null) {
			return;
		}

		int packageCount = (int)Math.ceil(bytes.length / (262144.0 - 8.0));
		int messageId = (int)Math.floor(Math.random() * Integer.MAX_VALUE);
		System.out.println("ID " + messageId);
		System.out.println("ID " + packageCount);
		byte[] messageIdBytes = ByteBuffer.allocate(4).putInt(messageId).array();
		for(int i = 0; i < packageCount; i++){
			byte[] packageIdBytes = ByteBuffer.allocate(4).putInt(i).array();
			byte[] dataBytes = Arrays.copyOfRange(bytes, (i*(262144 - 8)), Math.min(((i+1)*(262144 - 8))-1, bytes.length-1));
			try{
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
				outputStream.write(messageIdBytes);
				outputStream.write(packageIdBytes);
				outputStream.write(dataBytes);
				BytesMessage bm = new BytesMessage<>(outputStream.toByteArray(), this.sender(), message.getReceiver());
				receiverProxy.tell(bm, this.self());
			}catch(IOException e){
				e.printStackTrace();
			}

		}


	}

	public static int byteArrayToInt(byte[] b) {
		return b[3] & 0xFF | (b[2] & 0xFF) << 8 | (b[1] & 0xFF) << 16 | (b[0] & 0xFF) << 24;
	}

	private void handle(BytesMessage<?> message) {
		// Collect data
		byte[] messageData = (byte[])message.getBytes();
		System.out.println(messageData.length);
		int messageId = byteArrayToInt(Arrays.copyOfRange(messageData, 0, 4));
		int packageId = byteArrayToInt(Arrays.copyOfRange(messageData, 4, 8));
		System.out.println(messageId + " " + packageId);
		return;


		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		/*ByteArrayInputStream bis = new ByteArrayInputStream(messageData);
		ObjectInput in = null;
		Object o = null;
		try {
			in = new ObjectInputStream(bis);
			o = in.readObject();

			System.out.println(o.getClass().getName());
		} catch(IOException e) {
		} catch(ClassNotFoundException f) {

		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}

		// Redirect message
		BytesMessage bm = new BytesMessage<>(o, this.sender(), message.getReceiver());
		message.getReceiver().tell(bm.getBytes(), message.getSender());*/
	}
}
