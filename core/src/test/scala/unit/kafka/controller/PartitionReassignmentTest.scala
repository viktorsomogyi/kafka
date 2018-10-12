/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class PartitionReassignmentTest  extends JUnitSuite {

  @Test
  def testCalculateReassignmentStep(): Unit = {
    assertEquals(
      ReassignmentStep(drop = Seq.empty,add =Some(2), newReplicas = Seq(0, 1, 2)),
      ReassignmentHelper.calculateReassignmentStep(Seq(2, 3), Seq(0, 1), 0))
    assertEquals(
      ReassignmentStep(drop = Seq(1),add =Some(3), newReplicas = Seq(0, 2, 3)),
      ReassignmentHelper.calculateReassignmentStep(Seq(2, 3), Seq(0, 1, 2), 0))
  }
}
