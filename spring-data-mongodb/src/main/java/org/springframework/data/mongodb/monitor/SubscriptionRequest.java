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

import org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions;

/**
 * The actual {@link SubscriptionRequest} sent to the {@link MessageListenerContainer}. This wrapper type allows passing
 * in {@link RequestOptions additional information} to the Container which can be used for creating the actual
 * {@link Task} running. <br />
 * The {@link MessageListener} provides the callback interface when pushing {@link Message massages}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
interface SubscriptionRequest<M extends Message, O extends RequestOptions> {

	/**
	 * @return never {@literal null}.
	 */
	MessageListener<M> getMessageListener();

	/**
	 * @return
	 */
	O getRequestOptions();

	static interface RequestOptions {

		String getCollectionName();

	}
}
