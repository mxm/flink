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

package org.apache.flink.runtime.messages

/**
 * Generic messages between JobManager, TaskManager, JobClient.
 */
// TODO what about this one?
object Messages {

  /**
   * Message to signal the successful reception of another message
   */
  object Acknowledge

  /**
    * Accessor for the case object instance, to simplify Java interoperability.
    *
    * @return The Acknowledge case object instance.
    */
  def getAcknowledge(): Acknowledge.type = Acknowledge

  /**
   * Signals that the receiver (TaskManager) shall disconnect from the ResourceManager.
   *
   * The ResourceManager may send this message to its TaskManagers to let them clean up their
   * tasks that depend on the JobManager and go into a clean state.
   *
   * @param reason The reason for disconnecting, to be displayed in log and error messages.
   */
  case class Disconnect(reason: String) extends RequiresLeaderSessionID
}
