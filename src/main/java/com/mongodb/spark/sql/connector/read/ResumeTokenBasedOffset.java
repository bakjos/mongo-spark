/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.spark.sql.connector.read;

import static com.mongodb.spark.sql.connector.schema.ConverterHelper.toJson;

import com.mongodb.client.ChangeStreamIterable;
import org.bson.BsonDocument;

final class ResumeTokenBasedOffset extends MongoOffset {

  private static final long serialVersionUID = 1L;

  private final BsonDocument resumeToken;

  ResumeTokenBasedOffset(final BsonDocument resumeToken) {
    this.resumeToken = resumeToken;
  }

  @Override
  String getOffsetJsonValue() {
    return toJson(resumeToken);
  }

  @Override
  <T> ChangeStreamIterable<T> applyToChangeStreamIterable(
      final ChangeStreamIterable<T> changeStreamIterable) {
    return changeStreamIterable.resumeAfter(resumeToken);
  }
}
