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
 * Interface for agreements produced by {@link CoordinationEngine}.
 * @param L is the learner type, which processes the agreement.
 * @param R is the value returned after the agreement is executed.
 */
public interface Agreement<L, R> extends Serializable {

  /**
   * An agreement in the Global Sequence of events is characterized by
   * the global sequence number with the monotonicity property
   * that agreements with higher GSN are applied later in the sequence.
   * The agreement has the same GSN when it is learned on all nodes.
   *
   * @return global sequence number of the agreement
   */
  long getGlobalSequenceNumber();

  /**
   * A call-back that {@link CoordinationEngine} makes for each agreement.
   * Every agreement type should know how to execute the command.
   *
   * @throws IOException
   */
  public abstract R execute(final L callBackObject) throws IOException;
}
