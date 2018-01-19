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

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.test.util.Assertions;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;

/**
 * @author Christoph Strobl
 */
public class ChangeStreamTests {

	public static final String DATABASE_NAME = "change-stream-events";
	public static final String COLLECTION_NAME = "collection-1";
	MongoDbFactory dbFactory;

	MongoCollection<Document> collection;
	private CollectingMessageListener messageListener;
	private MongoTemplate template;

	public static @ClassRule TestRule replSet = new TestRule() {

		boolean replSet = false;

		{
			try (MongoClient client = new MongoClient()) {
				replSet = client.getDatabase("admin").runCommand(new Document("getCmdLineOpts", "1")).get("argv", List.class)
						.contains("--replSet");
			}
		}

		@Override
		public Statement apply(Statement base, Description description) {

			return new Statement() {

				@Override
				public void evaluate() throws Throwable {

					if (!replSet) {
						throw new AssumptionViolatedException("Not runnig in repl set mode");
					}
					base.evaluate();
				}
			};
		}
	};

	@Before
	public void setUp() {

		dbFactory = new SimpleMongoDbFactory(new MongoClient(), DATABASE_NAME);
		template = new MongoTemplate(dbFactory);

		template.dropCollection(COLLECTION_NAME);
		collection = template.getCollection(COLLECTION_NAME);

		messageListener = new CollectingMessageListener();
	}

	@Test // DATAMONGO-1803
	public void shouldOnlyReceiveMessagesWhileActive() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.register(new ChangeStreamRequest(messageListener, () -> COLLECTION_NAME), Document.class);
		container.start();

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(200);

		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));

		Thread.sleep(200);

		Assertions.assertThat(messageListener.getTotalNumberMessagesReceived()).isEqualTo(2);
	}

	@Test // DATAMONGO-1803
	public void mapping() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);

		MongoTemplate template = new MongoTemplate(dbFactory);

		// TODO: find a better way for messages and conversion.
		container.register(new ChangeStreamRequest<Person>(message -> System.out.println("person: " + message.getBody()),
				() -> COLLECTION_NAME), Person.class);
		container.start();

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("firstname", "foo"));
		collection.insertOne(new Document("_id", "id-2").append("lastname", "bar"));

		Thread.sleep(200);

		container.stop();
	}

	@Test
	public void startAndListenToChangeStream() throws InterruptedException {

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.start();
		container.register(new ChangeStreamRequest(System.out::println, () -> COLLECTION_NAME), Document.class);

		Thread.sleep(200);

		collection.insertOne(new Document("_id", "id-1").append("value", "foo"));

		Thread.sleep(1000);
		collection.insertOne(new Document("_id", "id-2").append("value", "bar"));

		Thread.sleep(1000);
		container.stop();

		collection.insertOne(new Document("_id", "id-3").append("value", "bar"));
		Thread.sleep(2000);
		collection.insertOne(new Document("_id", "id-4").append("value", "bar"));
		Thread.sleep(2000);
	}

	@Test
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

	@Test
	public void testInfiniteStreams() throws InterruptedException {

		dbFactory.getDb().createCollection(COLLECTION_NAME,
				new CreateCollectionOptions().capped(true).maxDocuments(10000).sizeInBytes(10000));

		MessageListenerContainer container = new DefaultMessageListenerContainer(template);
		container.register(new TailableCursorRequest(System.out::println, () -> COLLECTION_NAME), Document.class);

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

	static class CollectingMessageListener<M extends Message> implements MessageListener<M> {

		List<M> messages = new ArrayList<>();

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
	}
}
