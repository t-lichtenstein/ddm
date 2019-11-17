package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.Char;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class CrackedPasswordMessage implements Serializable {
		private static final long serialVersionUID = 3304081601659723998L;
		private int id;
		private String password;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private List<Character> crackedHints = new ArrayList<>();
	private List<String> hintHashes;
	private boolean enoughHintsFound = false;
	private String crackedPassword = "";

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.CrackMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(Master.CrackMessage message) {
		this.log().info("Received password hash to crack with id " + message.getId());

		// Phase 1: Hint cracking
		this.hintHashes = new ArrayList(Arrays.asList(message.getHints()));
		this.crackedHints = new ArrayList<>();
		// Copy is necessary because otherwise the value is shared among other worker instances on the same system
		char[] alphabet = Arrays.copyOf(message.getCharacters(), message.getCharacters().length);
		heapPermutation(alphabet, alphabet.length, alphabet.length, message.getHintsToCrack());
		this.enoughHintsFound = false;
		this.log().info("Cracked " + this.crackedHints.size() + " hints for password id: " + message.getId());

		List<Character> passwordAlphabet = new ArrayList<>();

		for (char character : alphabet) {
			if (!this.crackedHints.contains(character)) {
				passwordAlphabet.add(character);
			}
		}

		// Phase 2: Crack password
		this.crackedPassword = "";
		generateCombinations(
				passwordAlphabet,
				message.getLength(),
				"",
				passwordAlphabet.size(),
				message.getPassword());

		if (this.crackedPassword.equals("")) {
			this.log().error("Password " + message.getId() + " could not be cracked");
		} else {
			this.log().info("Found password " + this.crackedPassword + " for id: " + message.getId());
			CrackedPasswordMessage answer = new CrackedPasswordMessage(message.getId(), this.crackedPassword);
			this.sender().tell(answer, this.self());
		}
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void generateCombinations(List<Character> alphabet, int length, String prefix, int n, String passwordHash)  {
		if (!this.crackedPassword.equals("")) {
			return;
		}

		if (length == 0) {
			if (hash(prefix).equals(passwordHash)) {
				this.crackedPassword = prefix;
			}
			return;
		}

		for (int i = 0; i < n; i++) {
			String newPrefix = (prefix + alphabet.get(i));
			generateCombinations(alphabet, length - 1, newPrefix, n, passwordHash);
		}
	}

	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private void checkPermutation(String permutation) {
		char possibleHint = permutation.charAt(permutation.length() - 1);
		String currentHintHash = hash(permutation.substring(0, permutation.length() - 1));
		for (int i = 0; i < this.hintHashes.size(); i++) {
			if (currentHintHash.equals(this.hintHashes.get(i))) {
				this.hintHashes.remove(i);
				this.crackedHints.add(possibleHint);
				this.log().info("Cracked hint number " + this.crackedHints.size() + " : " + possibleHint);
				break;
			}
		}
	}

	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, int hintsToCrack) {
		// If enough hints are found, exit the algorithm
		if (this.enoughHintsFound) {
			return;
		}

		// If size is 1, store the obtained permutation
		if (size == 1) {
			this.checkPermutation(new String(a));
			if (this.crackedHints.size() >= hintsToCrack) {
				this.enoughHintsFound = true;
				return;
			}
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, hintsToCrack);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}