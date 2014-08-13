/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.coordination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for agreements produced by a {@link CoordinationEngine}.
 */
public interface Agreement<L, R> extends Serializable {

  /**
   * Execute the agreement on the learner of type {@link L}.
   *
   * @param learner      the learner the agreement will be applied to.
   * @param proposeIdentity identity of the propose which delivered agreement
   * @param ceIdentity   identity of the CE engine created agreement
   * @return             the result of applying the agreement to the learner.
   *                     This may be {@link Void} if there is no result. This
   *                     result is used to inform the client who originally
   *                     requested (proposed) the update to the learner.
   * @throws IOException if there was a problem applying the
   *                     agreement to the learner.
   */
  abstract R execute(String proposeIdentity, String ceIdentity, final L learner)
          throws IOException;
}
