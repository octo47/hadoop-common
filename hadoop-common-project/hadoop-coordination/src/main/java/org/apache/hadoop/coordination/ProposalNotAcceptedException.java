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

/**
 * Thrown by {@link org.apache.hadoop.coordination.CoordinationEngine)
 * when submitted proposal was not accepted for processing.
 * <p>
 * Reasons for a proposal being not accepted are specific
 * for the CoordinationEngine implementation.
 * <p>
 * A common reason is that a competing proposal, typically with the same GSN,
 * has already been submitted to the engine.
 */
public class ProposalNotAcceptedException extends IOException {
  private static final long serialVersionUID = -8981758542101961950L;

  public ProposalNotAcceptedException(final String message) {
    super(message);
  }

  public ProposalNotAcceptedException(String message, final Throwable t) {
    super(message, t);
  }
}
