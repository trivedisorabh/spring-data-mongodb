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

import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

/**
 * {@link MessageConverter} take the raw {@link Message} an provide means to convert the {@link Message#getRaw() raw
 * message} into a desired target type. The main purpose is its usage with
 * {@link org.springframework.data.mongodb.monitor.Message.ConvertibleMessage}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface MessageConverter {

	/**
	 * Convert a given {@link Message} into the desired {@code type}.
	 * 
	 * @param message must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	<T> T convert(Message<?> message, Class<T> type);

	static MessageConverter mapping(MongoConverter converter) {

		return new MessageConverter() {

			@Override
			public <T> T convert(Message<?> message, Class<T> type) {

				if (message.getRaw() instanceof org.bson.Document) {
					return converter.read(type, (org.bson.Document) message.getRaw());
				}
				if (message.getRaw() instanceof ChangeStreamDocument) {
					return converter.read(type, ((ChangeStreamDocument<org.bson.Document>) message.getRaw()).getFullDocument());
				}

				throw new IllegalArgumentException();
			}
		};
	}

	/**
	 * @return
	 */
	static MessageConverter none() {

		return new MessageConverter() {
			@Override
			public <T> T convert(Message<?> message, Class<T> type) {

				Assert.notNull(message, "Message must not be null!");
				Assert.notNull(type, "Type must not be null!");

				return type.cast(message.getRaw());
			}
		};
	}
}
