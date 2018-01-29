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
package org.springframework.data.mongodb.core;

import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Optional;

import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.FullDocument;

/**
 * Options applicable to MongoDB <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a>. Intended
 * to be used along with {@link org.springframework.data.mongodb.core.ChangeStreamRequest.ChangeStreamRequestOptions} in
 * a sync world as well {@link ReactiveMongoOperations} if you prefer it that way.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@EqualsAndHashCode
public class ChangeStreamOptions {

	private @Nullable Object filter;
	private @Nullable BsonValue resumeToken;
	private @Nullable FullDocument fullDocumentLookup;
	private @Nullable Collation collation;

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

	/**
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

	/**
	 * @return empty {@link ChangeStreamOptions}.
	 */
	public static ChangeStreamOptions empty() {
		return ChangeStreamOptions.builder().build();
	}

	/**
	 * Obtain a shiny new {@link ChangeStreamRequestOptionsBuilder} and start defining options in this fancy fluent way.
	 * Just don't forget to call {@link ChangeStreamRequestOptionsBuilder#build() build()} when your're done.
	 *
	 * @return new instance of {@link ChangeStreamRequestOptionsBuilder}.
	 */
	public static ChangeStreamOptionsBuilder builder() {
		return new ChangeStreamOptionsBuilder();
	}

	/**
	 * Builder for creating {@link ChangeStreamOptions}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ChangeStreamOptionsBuilder {

		private ChangeStreamOptions options = new ChangeStreamOptions();

		/**
		 * Set the collation to use.
		 *
		 * @param collation must not be {@literal null} nor {@literal empty}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder collation(Collation collation) {

			Assert.notNull(collation, "Collation must not be null nor empty!");

			options.collation = collation;
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
		public ChangeStreamOptionsBuilder filter(Aggregation filter) {

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
		public ChangeStreamOptionsBuilder filter(Document... filter) {

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
		public ChangeStreamOptionsBuilder resumeToken(BsonValue resumeToken) {

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
		public ChangeStreamOptionsBuilder returnFullDocumentOnUpdate() {
			return fullDocumentLookup(FullDocument.UPDATE_LOOKUP);
		}

		/**
		 * Set the {@link FullDocument} lookup to use.
		 *
		 * @param lookup must not be {@literal null}.
		 * @return this.
		 */
		public ChangeStreamOptionsBuilder fullDocumentLookup(FullDocument lookup) {

			Assert.notNull(lookup, "Lookup must not be null!");
			options.fullDocumentLookup = lookup;
			return this;
		}

		public ChangeStreamOptions build() {

			ChangeStreamOptions tmp = options;
			options = new ChangeStreamOptions();
			return tmp;
		}
	}
}
