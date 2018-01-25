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

import java.util.Arrays;
import java.util.Optional;

import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * {@link SubscriptionRequest} implementation to be used for listening to
 * <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a> via a {@link MessageListenerContainer}
 * using the synchronous MongoDB Java driver.
 * <p/>
 * The most trivial use case is subscribing to all events of a specific {@link com.mongodb.client.MongoCollection
 * collection}.
 * 
 * <pre>
 * <code>
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, () -> "collection-name");
 * </code>
 * </pre>
 * 
 * For more advanced scenarios {@link ChangeStreamRequestOptions} offers abstractions for options like filtering,
 * resuming,...
 * 
 * <pre>
 * <code>
 *     ChangeStreamRequestOptions options = ChangeStreamRequestOptions.builder()
 *     		.collection("collection-name")
 *     		.returnFullDocumentOnUpdate()
 *     		.build();
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, options);
 * </code>
 * </pre>
 * 
 * {@link Message Messges} passed to the {@link MessageListener} contain the {@link ChangeStreamDocument} within their
 * {@link Message#getRaw() raw value} while the {@code fullDocument} is extracted into the {@link Message#getBody()
 * messages body}. Unless otherwise specified (via {@link ChangeStreamRequestOptions#getFullDocumentLookup()} the
 * {@link Message#getBody() message body} for {@code update events} will be empty for a {@link Document} target type.
 * {@link Message#getBody()} Message bodies} that map to a different target type automatically enforce an
 * {@link FullDocument#UPDATE_LOOKUP}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class ChangeStreamRequest<T>
		implements SubscriptionRequest<Message<ChangeStreamDocument<Document>, T>, ChangeStreamRequestOptions> {

	private final MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener;
	private final ChangeStreamRequestOptions options;

	/**
	 * Create a new {@link ChangeStreamRequest} with options, passing {@link Message messages} to the given
	 * {@link MessageListener}.
	 *
	 * @param messageListener must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	public ChangeStreamRequest(MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener,
			RequestOptions options) {

		Assert.notNull(messageListener, "MessageListener must not be null!");
		Assert.notNull(options, "Options must not be null!");

		this.options = options instanceof ChangeStreamRequestOptions ? (ChangeStreamRequestOptions) options
				: ChangeStreamRequestOptions.of(options);

		this.messageListener = messageListener;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getMessageListener()
	 */
	@Override
	public MessageListener<Message<ChangeStreamDocument<Document>, T>> getMessageListener() {
		return messageListener;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getRequestOptions()
	 */
	@Override
	public ChangeStreamRequestOptions getRequestOptions() {
		return options;
	}

	/**
	 * {@link org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions} implementation specific to a
	 * {@link ChangeStreamRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ChangeStreamRequestOptions
			implements org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions {

		private String collectionName;
		private @Nullable Object filter;
		private @Nullable BsonValue resumeToken;
		private @Nullable FullDocument fullDocumentLookup;

		static ChangeStreamRequestOptions of(RequestOptions options) {
			return builder().collection(options.getCollectionName()).build();
		}

		/**
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<Object> getFilter() {
			return Optional.ofNullable(filter);
		}

		/**
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<BsonValue> getResumeToken() {
			return Optional.ofNullable(resumeToken);
		}

		/**
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<FullDocument> getFullDocumentLookup() {
			return Optional.ofNullable(fullDocumentLookup);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions#getCollectionName()
		 */
		@Override
		public String getCollectionName() {
			return collectionName;
		}

		/**
		 * Obtain a shiny new {@link ChangeStreamRequestOptionsBuilder} and start defining options in this fancy fluent way.
		 * Just don't forget to call {@link ChangeStreamRequestOptionsBuilder#build() build()} when your're done.
		 *
		 * @return new instance of {@link ChangeStreamRequestOptionsBuilder}.
		 */
		public static ChangeStreamRequestOptionsBuilder builder() {
			return new ChangeStreamRequestOptionsBuilder();
		}

		/**
		 * Builder for creating {@link ChangeStreamRequestOptions}.
		 *
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		public static class ChangeStreamRequestOptionsBuilder {

			private ChangeStreamRequestOptions options = new ChangeStreamRequestOptions();

			/**
			 * Set the collection name to listen to.
			 *
			 * @param collection must not be {@literal null} nor {@literal empty}.
			 * @return this.
			 */
			public ChangeStreamRequestOptionsBuilder collection(String collection) {

				Assert.hasText(collection, "Collection must not be null nor empty!");

				options.collectionName = collection;
				return this;
			}

			/**
			 * Set the filter to apply.
			 * <p/>
			 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to ensure filter expressions are
			 * mapped to domain type fields.
			 *
			 * @param filter the {@link Aggregation Aggregation pipeline} to apply for filtering events. Must not be
			 *          {@literal null}.
			 * @return this.
			 */
			public ChangeStreamRequestOptionsBuilder filter(Aggregation filter) {

				Assert.notNull(filter, "Filter must not be null!");

				options.filter = filter;
				return this;
			}

			/**
			 * Set the plain filter chain to apply.
			 *
			 * @param filter must not be {@literal null} nor contain {@literal null} values.
			 * @return this.
			 */
			public ChangeStreamRequestOptionsBuilder filter(Document... filter) {

				Assert.noNullElements(filter, "Filter must not contain null values");
				options.filter = Arrays.asList(filter);
				return this;
			}

			/**
			 * Set the resume token (typically a {@link org.bson.BsonDocument} containing a {@link org.bson.BsonBinary binary
			 * token}) after which to start with listening.
			 *
			 * @param resumeToken must not be {@literal null}.
			 * @return this.
			 */
			public ChangeStreamRequestOptionsBuilder resumeToken(BsonValue resumeToken) {

				Assert.notNull(resumeToken, "ResumeToken must not be null!");
				options.resumeToken = resumeToken;
				return this;
			}

			/**
			 * Set the {@link FullDocument} lookup to {@link FullDocument#UPDATE_LOOKUP}.
			 *
			 * @return this.
			 * @see #fullDocumentLookup(FullDocument)
			 */
			public ChangeStreamRequestOptionsBuilder returnFullDocumentOnUpdate() {
				return fullDocumentLookup(FullDocument.UPDATE_LOOKUP);
			}

			/**
			 * Set the {@link FullDocument} lookup to use.
			 *
			 * @param lookup must not be {@literal null}.
			 * @return this.
			 */
			public ChangeStreamRequestOptionsBuilder fullDocumentLookup(FullDocument lookup) {

				Assert.notNull(lookup, "Lookup must not be null!");
				options.fullDocumentLookup = lookup;
				return this;
			}

			public ChangeStreamRequestOptions build() {

				ChangeStreamRequestOptions tmp = options;
				options = new ChangeStreamRequestOptions();
				return tmp;
			}
		}
	}
}
