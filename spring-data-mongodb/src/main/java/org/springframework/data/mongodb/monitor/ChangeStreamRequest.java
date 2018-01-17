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

import org.bson.Document;
import org.springframework.data.mongodb.monitor.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * @author Christoph Strobl
 * @since 2.1
 */
public class ChangeStreamRequest
		implements SubscriptionRequest<Message<ChangeStreamDocument<Document>>, ChangeStreamRequestOptions> {

	private final MessageListener<Message<ChangeStreamDocument<Document>>> messageListener;
	private final ChangeStreamRequestOptions options;

	public ChangeStreamRequest(MessageListener<Message<ChangeStreamDocument<Document>>> messageListener,
			RequestOptions options) {

		Assert.notNull(messageListener, "MessageListener must not be null!");
		Assert.notNull(options, "Options must not be null!");

		this.options = options instanceof ChangeStreamRequestOptions ? (ChangeStreamRequestOptions) options
				: new ChangeStreamRequestOptions(options);

		this.messageListener = messageListener;
	}

	@Override
	public MessageListener<Message<ChangeStreamDocument<Document>>> getMessageListener() {
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
		private @Nullable Document filter;
		private @Nullable Document resumeToken;
		private @Nullable MessageConverter converter;

		private ChangeStreamRequestOptions() {

		}

		private ChangeStreamRequestOptions(RequestOptions options) {
			this.collectionName = options.getCollectionName();
		}

		@Override
		public String getCollectionName() {
			return collectionName;
		}

		public Document getFilter() {
			return filter;
		}

		public Document getResumeToken() {
			return resumeToken;
		}

		@Nullable
		public MessageConverter getConverter() {
			return converter;
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

			public ChangeStreamRequestOptionsBuilder collection(Document filter) {
				options.filter = filter;
				return this;
			}

			public ChangeStreamRequestOptionsBuilder resumeToken(Document resumeToken) {
				options.resumeToken = resumeToken;
				return this;
			}

			public ChangeStreamRequestOptionsBuilder converter(MessageConverter converter) {
				options.converter = converter;
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
