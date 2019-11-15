package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}

	@Data
	public static class StartPollingCrackMessages implements Serializable {
		private static final long serialVersionUID = -50374816448627601L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class CrackMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723998L;
		private int id;
		private String password;
		private int length;
		private char[] characters;
		private String[] hints;
		private int hintsToCrack;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private int passwordCounter = 0;
	private final Queue<ActorRef> idleWorkers = new LinkedBlockingQueue<>();
	private final Queue<CrackMessage> messages = new LinkedBlockingQueue<>();

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Worker.CrackedPasswordMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	private long recfact(long start, long n) {
		long i;
		if (n <= 16) {
			long r = start;
			for (i = start + 1; i < start + n; i++) r *= i;
			return r;
		}
		i = n / 2;
		return recfact(start, i) * recfact(start + i, n - i);
	}
	long factorial(long n) { return recfact(1, n); }
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		if (message.getLines().isEmpty()) {
			this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
			// this.terminate();
			return;
		}

		List<String[]> lines = message.getLines();
		passwordCounter += lines.size();
		String[] firstEntry = lines.get(0);
		final int firstHintIndex = 5;

		char[] characters = firstEntry[2].toCharArray();
		int characterAmount = firstEntry[2].length();
		int passwordLength = Integer.parseInt(firstEntry[3]);
		int numberOfHints = firstEntry.length - firstHintIndex;
		int numberOfHintsToCrack = 0;
		while (numberOfHintsToCrack < numberOfHints) {
			if ((long) Math.pow((characterAmount - numberOfHintsToCrack), passwordLength) <
					(factorial(characterAmount) / factorial(characterAmount - passwordLength - 1))) {
				break;
			}
			numberOfHintsToCrack++;
		}

		for (String[] line : lines) {
			int id = Integer.parseInt(firstEntry[0]);
			String password = line[4];
			String[] hints = Arrays.copyOfRange(line, firstHintIndex, line.length);
			this.messages.add(new CrackMessage(id, password, passwordLength, characters, hints, numberOfHintsToCrack));
		}

		while (this.idleWorkers.size() > 0) {
			this.idleWorkers
					.remove()
					.tell(this.messages.remove(), this.self());
		}

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		if (this.messages.size() != 0) {
			this.sender().tell(this.messages.remove(), this.self());
		} else {
			this.idleWorkers.add(this.sender());
		}
//		this.log().info("Registered {}", this.sender());
	}

	protected void handle(Worker.CrackedPasswordMessage message) {
		passwordCounter--;
		this.collector.tell(new Collector.CollectMessage(
				"ID: " + message.getId() + " | Password: " + message.getPassword()
		), this.self());
		if (passwordCounter == 0 && this.messages.size() == 0) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		} else {
			if (this.messages.size() != 0) {
				this.sender().tell(this.messages.remove(), this.self());
			} else {
				this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
