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

import org.springframework.context.ApplicationEventPublisher;

/**
 * Listener interface to receive asynchronous delivery of {@link Message Messages}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@FunctionalInterface
interface MessageListener<M extends Message> {

	void onMessage(M message);

	static class DecoratingEventPublishingMessageListener<T extends Message> implements MessageListener<T> {

		private final ApplicationEventPublisher publisher;
		private final MessageListener<T> delegate;

		public DecoratingEventPublishingMessageListener(MessageListener<T> delegate, ApplicationEventPublisher publisher) {

			this.publisher = publisher;
			this.delegate = delegate;
		}

		@Override
		public void onMessage(T message) {

			// TODO: pass on event via publisher
			this.delegate.onMessage(message);
		}
	}
}
