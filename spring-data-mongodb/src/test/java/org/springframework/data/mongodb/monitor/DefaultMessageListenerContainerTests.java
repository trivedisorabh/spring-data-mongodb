/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.monitor;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.test.annotation.IfProfileValue;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;

/**
 * Integration tests for {@link DefaultMessageListenerContainer}.
 *
 * @author Christoph Strobl
 */
public class DefaultMessageListenerContainerTests {

	public static final String DATABASE_NAME = "change-stream-events";
	public static final String COLLECTION_NAME = "collection-1";
	MongoDbFactory dbFactory;

	MongoCollection<Document> collection;
	private CollectingMessageListener<Message> messageListener;
	private MongoTemplate template;

	public @Rule TestRule replSet = ReplicaSet.none();

	@Before
	public void setUp() {

		dbFactory = new SimpleMongoDbFactory(new MongoClient(), DATABASE_NAME);
		template = new MongoTemplate(dbFactory);

		template.dropCollection(COLLECTION_NAME);
		collection = template.getCollection(COLLECTION_NAME);

		messageListener = new CollectingMessageListener();
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldCollectMappedChangeStreamMessagesCorrectly() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Person.class);
		container.start();

		awaitSubscription(subscription, Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("firstname", "bar"));

		awaitMessages(2, Duration.ofMillis(500));

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(new Person("id-1", "foo"), new Person("id-2", "bar"));

	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldNoLongerReceiveMessagesWhenConainerStopped() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);
		container.start();

		awaitSubscription(subscription, Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(2, Duration.ofMillis(500));

		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));

		Thread.sleep(200);

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void shouldReceiveMessagesWhenAddingRequestToAlreadyStartedContainer() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		Document unexpected = new Document("_id", "id-1").append("value", "foo");
		collection.insertOne(unexpected);

		Subscription subscription = container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME),
				Document.class);

		awaitSubscription(subscription, Duration.ofMillis(500));

		Document expected = new Document("_id", "id-2").append("value", "bar");
		collection.insertOne(expected);

		awaitMessages(1, Duration.ofMillis(500));
		container.stop();

		assertThat(messageListener.getMessages().stream().map(Message::getBody).collect(Collectors.toList()))
				.containsExactly(expected);
	}

	@Test // DATAMONGO-1803
	@IfProfileValue(name = "replSet", value = "true")
	public void startAndListenToChangeStreamOther() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.register(new ChangeStreamRequest(System.out::println, () -> COLLECTION_NAME), Document.class);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(1000);

		container.start();

		Thread.sleep(500);

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(1000);
		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));
		Thread.sleep(2000);
	}

	@Test // DATAMONGO-1803
	public void tailableCursor() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(
				container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME), Document.class),
				Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(2, Duration.ofSeconds(2));
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void tailableCursorOnEmptyCollection() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();

		awaitSubscription(
				container.register(new TailableCursorRequest(messageListener, () -> COLLECTION_NAME), Document.class),
				Duration.ofMillis(500));

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		awaitMessages(2, Duration.ofSeconds(2));
		container.stop();

		assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	static class CollectingMessageListener<M extends Message> implements MessageListener<M> {

		volatile List<M> messages = new ArrayList<>();

		@Override
		public void onMessage(M message) {
			messages.add(message);
		}

		int getTotalNumberMessagesReceived() {
			return messages.size();
		}

		public List<M> getMessages() {
			return messages;
		}
	}

	@Data
	static class Person {
		@Id String id;
		private String firstname;
		private String lastname;

		public Person() {}

		public Person(String id, String firstname) {
			this.id = id;
			this.firstname = firstname;
		}
	}

	private void awaitSubscription(Subscription subscription, Duration max) throws InterruptedException {

		long passedMs = 0;
		long maxMs = max.toMillis();
		while (!subscription.isActive() && passedMs < maxMs) {
			Thread.sleep(10);
			passedMs += 10;
		}
	}

	private void awaitMessages(int nrMessages, Duration max) throws InterruptedException {

		long passedMs = 0;
		long maxMs = max.toMillis();
		while (messageListener.getTotalNumberMessagesReceived() < nrMessages && passedMs < maxMs) {
			Thread.sleep(10);
			passedMs += 10;
		}
	}
}
