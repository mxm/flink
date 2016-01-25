///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.runtime.clusterframework.messages;
//
//import akka.actor.ActorRef;
//import org.apache.flink.runtime.clusterframework.types.ResourceID;
//import org.apache.flink.runtime.instance.HardwareDescription;
//import org.apache.flink.runtime.instance.InstanceConnectionInfo;
//import org.apache.flink.runtime.instance.InstanceID;
//import org.apache.flink.runtime.messages.RequiresLeaderSessionID;
//
//import java.io.Serializable;
//
//import static java.util.Objects.requireNonNull;
//
///**
// * Message sent to the JobManager by the TaskManager to request the resource manager URL.
// */
//public class LookupResourceManager implements RequiresLeaderSessionID, Serializable {
//
//	private static final long serialVersionUID = 42L;
//
//	public LookupResourceManager() {}
//
//}
