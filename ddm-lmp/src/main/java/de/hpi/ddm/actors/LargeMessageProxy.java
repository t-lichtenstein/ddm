package de.hpi.ddm.actors;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.HashMap;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////
	// Message Buffer //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	private class LargeMessagePackageCollection {
		public int packageCounter;
		public int packageAmount;
		public byte[][] payload;

		public void incrementPackageCount(){
			this.packageCounter++;
		}

		public void setPayload(int packageId, byte[] packageData){
			this.payload[packageId] = packageData;
		}
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
			System.out.println("Initial serialzed message length: " + bytes.length);
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

		final int payloadBytes = 262144 - 12;
		int packageCount = (int)Math.ceil(bytes.length / (double) payloadBytes);
		int messageId = (int)Math.floor(Math.random() * Integer.MAX_VALUE);
		System.out.println("Sender ID " + messageId);
		System.out.println("Sender Package Count: " + packageCount);
		byte[] messageIdBytes = ByteBuffer.allocate(4).putInt(messageId).array();
		byte[] packageCountBytes = ByteBuffer.allocate(4).putInt(packageCount).array();
		for(int i = 0; i < packageCount; i++){
			byte[] packageIdBytes = ByteBuffer.allocate(4).putInt(i).array();
			byte[] dataBytes = Arrays.copyOfRange(bytes, (i*payloadBytes), Math.min(((i+1)*payloadBytes), bytes.length));
			try{
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
				outputStream.write(messageIdBytes);
				outputStream.write(packageCountBytes);
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
		int messageId = byteArrayToInt(Arrays.copyOfRange(messageData, 0, 4));
		int packageCount = byteArrayToInt(Arrays.copyOfRange(messageData, 4, 8));
		int packageId = byteArrayToInt(Arrays.copyOfRange(messageData, 8, 12));
		byte[] messagePayload = Arrays.copyOfRange(messageData, 12, messageData.length);

		LargeMessagePackageCollection collection = largeMessageBuffer.get(messageId);
		if(collection == null){
			collection = new LargeMessagePackageCollection(0, packageCount, new byte[packageCount][]);
			largeMessageBuffer.put(messageId, collection);
		}
		collection.incrementPackageCount();
		collection.setPayload(packageId, messagePayload);

		if(collection.getPackageCounter() == packageCount) {
			byte[] finalMessage = null;
			try {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				for (byte[] entry : collection.getPayload()) {
					outputStream.write(entry);
				}
				finalMessage = outputStream.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
			ByteArrayInputStream bis = new ByteArrayInputStream(finalMessage);
			ObjectInput in = null;
			Object o = null;
			System.out.println(finalMessage.length);
			try {
				in = new ObjectInputStream(bis);
				o = in.readObject();

				BytesMessage bm = new BytesMessage<>(o, this.sender(), message.getReceiver());
				message.getReceiver().tell(bm.getBytes(), message.getSender());

				//System.out.println(o.getClass().getName());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException f) {
				f.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException ex) {
					// ignore close exception
				}
			}
		}
	}
}
