/*
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

package org.apache.hadoop.coordination.zk.bench;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * @author Andrey Stepachev
 */
public class MetricsStdoutSink implements MetricsSink {

  @Override
  public void putMetrics(MetricsRecord record) {
    System.out.print(record.timestamp());
    System.out.print(" ");
    System.out.print(record.context());
    System.out.print(".");
    System.out.print(record.name());
    String separator = ": ";
    for (MetricsTag tag : record.tags()) {
      System.out.print(separator);
      separator = ", ";
      System.out.print(tag.name());
      System.out.print("=");
      System.out.print(tag.value());
    }
    for (AbstractMetric metric : record.metrics()) {
      System.out.print(separator);
      separator = ", ";
      System.out.print(metric.name());
      System.out.print("=");
      System.out.print(metric.value());
    }
    System.out.println();
  }

  @Override
  public void flush() {
    System.out.flush();
  }

  @Override
  public void init(SubsetConfiguration conf) {
  }
}
