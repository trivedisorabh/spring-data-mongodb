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

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * @author Christoph Strobl
 * @since 2.1
 */
public class ChangeStreamRequest<T>
		implements SubscriptionRequest<Message<ChangeStreamDocument<Document>, T>, ChangeStreamRequestOptions> {

	private final MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener;
	private final ChangeStreamRequestOptions options;

	public ChangeStreamRequest(MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener,
			RequestOptions options) {

		Assert.notNull(messageListener, "MessageListener must not be null!");
		Assert.notNull(options, "Options must not be null!");

		this.options = options instanceof ChangeStreamRequestOptions ? (ChangeStreamRequestOptions) options
				: new ChangeStreamRequestOptions(options);

		this.messageListener = messageListener;
	}

	@Override
	public MessageListener<Message<ChangeStreamDocument<Document>, T>> getMessageListener() {
		return messageListener;
	}

	@Override
	public ChangeStreamRequestOptions getRequestOptions() {
		return options;
	}

	/**
	 * {@link org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions} implementation.
	 */
	public static class ChangeStreamRequestOptions
			implements org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions {

		private String collectionName;
		private @Nullable Aggregation filter;
		private @Nullable Document resumeToken;

		private ChangeStreamRequestOptions() {

		}

		private ChangeStreamRequestOptions(RequestOptions options) {
			this.collectionName = options.getCollectionName();
		}

		@Override
		public String getCollectionName() {
			return collectionName;
		}

		public Optional<Aggregation> getFilter() {
			return Optional.ofNullable(filter);
		}

		public Optional<Document> getResumeToken() {
			return Optional.ofNullable(resumeToken);
		}

		public static ChangeStreamRequestOptionsBuilder builder() {
			return new ChangeStreamRequestOptionsBuilder();
		}

		public static class ChangeStreamRequestOptionsBuilder {

			ChangeStreamRequestOptions options = new ChangeStreamRequestOptions();

			public ChangeStreamRequestOptionsBuilder collection(String collection) {
				options.collectionName = collection;
				return this;
			}

			public ChangeStreamRequestOptionsBuilder collection(Aggregation filter) {
				options.filter = filter;
				return this;
			}

			public ChangeStreamRequestOptionsBuilder resumeToken(Document resumeToken) {
				options.resumeToken = resumeToken;
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
