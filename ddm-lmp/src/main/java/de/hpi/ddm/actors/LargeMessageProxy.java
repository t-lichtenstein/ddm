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
	private class LargeMessageBuffer {
		public int packageCounter;
		public int packageAmount;
		public byte[][] payload;

		public void setPayload(int packageId, byte[] packageData){
			this.payload[packageId] = packageData;
			this.packageCounter++;
		}
	}

	private HashMap<Integer, LargeMessageBuffer> largeMessageBuffer = new HashMap<>();

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

	public static int byteArrayToInt(byte[] b) {
		return b[3] & 0xFF | (b[2] & 0xFF) << 8 | (b[1] & 0xFF) << 16 | (b[0] & 0xFF) << 24;
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// Serialize Message
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutput objectOut = null;
		byte[] messageBytes = null;

		// Java Serializer
		try {
			objectOut = new ObjectOutputStream(baos);
			objectOut.writeObject(message.getMessage());
			objectOut.flush();
			messageBytes = baos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				baos.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		// Kryo Serializer - Not used because the result is 12,4 % bigger even with class registration
		/*
		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(false);
		Output output = new Output(baos);
		kryo.writeObject(output, message.getMessage());
		output.flush();
		messageBytes = baos.toByteArray();
		System.out.println(messageBytes.length);
		output.close();
		try {
			baos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/

		if (messageBytes == null) {
			return;
		}

		// Maximal message size: 262144 bytes
		final int metaInfoBytes = 12;
		final int payloadBytes = 260000 - metaInfoBytes;

		// Create package meta information (number of packages, package id)
		int packageCount = (int) Math.ceil(messageBytes.length / (double) payloadBytes);
		int messageId = (int) Math.floor(Math.random() * Integer.MAX_VALUE);

		byte[] messageIdBytes = ByteBuffer.allocate(4).putInt(messageId).array();
		byte[] packageCountBytes = ByteBuffer.allocate(4).putInt(packageCount).array();

		for (int i = 0; i < packageCount; i++) {
			byte[] packageIdBytes = ByteBuffer.allocate(4).putInt(i).array();

			// Split data into chunks (packages)
			byte[] dataBytes = Arrays
					.copyOfRange(messageBytes, (i * payloadBytes), Math.min(((i + 1) * payloadBytes), messageBytes.length));
			try {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				outputStream.write(messageIdBytes);
				outputStream.write(packageCountBytes);
				outputStream.write(packageIdBytes);
				outputStream.write(dataBytes);
				byte[] messagePackage = outputStream.toByteArray();

				// Send chunk (package) to receiver
				BytesMessage bm = new BytesMessage<>(messagePackage, this.sender(), message.getReceiver());
				receiverProxy.tell(bm, this.self());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handle(BytesMessage<?> message) {
		// Collect data
		byte[] messageData = (byte[])message.getBytes();

		// Read meta information
		int messageId = byteArrayToInt(Arrays.copyOfRange(messageData, 0, 4));
		int numberOfPackages = byteArrayToInt(Arrays.copyOfRange(messageData, 4, 8));
		int packageId = byteArrayToInt(Arrays.copyOfRange(messageData, 8, 12));

		// Read payload
		byte[] messagePayload = Arrays.copyOfRange(messageData, 12, messageData.length);

		// Get collection for message id
		LargeMessageBuffer collection = largeMessageBuffer.get(messageId);

		// Init message buffer for new message id if it does not exist
		if (collection == null) {
			collection = new LargeMessageBuffer(0, numberOfPackages, new byte[numberOfPackages][]);
			largeMessageBuffer.put(messageId, collection);
		}

		// Add package to buffer
		collection.setPayload(packageId, messagePayload);

		// Check whether message is complete
		if (collection.getPackageCounter() == numberOfPackages) {

			// Remove buffer entry to free the message id for future messages
			largeMessageBuffer.remove(messageId);

			// Reassemble the message
			byte[] completeMessage = null;
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				for (byte[] entry : collection.getPayload()) {
					baos.write(entry);
				}
				completeMessage = baos.toByteArray();
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Reassemble the message content
			ByteArrayInputStream bais = new ByteArrayInputStream(completeMessage);
			Object messageObject = null;

			// Java Deserializer
			ObjectInput objectInput = null;
			try {
				objectInput = new ObjectInputStream(bais);
				messageObject = objectInput.readObject();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException f) {
				f.printStackTrace();
			} finally {
				try {
					if (objectInput != null) {
						objectInput.close();
					}
				} catch (IOException ex) {
					// Ignore close exception
				}
			}

			// Kryo Deserializer - Not used because the result is 12,4% bigger even with class registration
			/*
			Kryo kryo = new Kryo();
			kryo.setRegistrationRequired(false);
			Input input = new Input(bais);
			messageObject = kryo.readClassAndObject(input);
			input.close();
			BytesMessage bm = new BytesMessage<>(o, this.sender(), message.getReceiver());
			message.getReceiver().tell(bm.getBytes(), message.getSender());
			*/

			// Redirect message to actual target
			BytesMessage bm = new BytesMessage<>(messageObject, this.sender(), message.getReceiver());
			message.getReceiver().tell(bm.getBytes(), message.getSender());
		}
	}
}
