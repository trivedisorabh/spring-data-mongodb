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
import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;

/**
 * @author Christoph Strobl
 * @since 2.1
 */
public class TailableCursorRequest<T> implements SubscriptionRequest<Message<Document, T>, RequestOptions> {

	private MessageListener<Message<Document, T>> messageListener;
	private RequestOptions options;

	public TailableCursorRequest(MessageListener<Message<Document, T>> messageListener, RequestOptions options) {
		this.messageListener = messageListener;
		this.options = options;
	}

	@Override
	public MessageListener<Message<Document, T>> getMessageListener() {
		return messageListener;
	}

	@Override
	public RequestOptions getRequestOptions() {
		return options;
	}
}
