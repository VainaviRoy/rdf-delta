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

package dev;

public class DevDeltaToDo {
    
    // Getting rpevious oatch
    //    DeltaConnection.attach
    //    DeltaConnection.connect
    //    DeltaConnection.create
    // Zone+DataState
    
    // Id.nil for no previous.
    
    // XXX remove DConn, DLink remote version getting. and only have PatchLogInfo. 
    
    // Start a DeltaConnection from an empty datasets
    //   Move spin up code to DLink.
    
    // Tests for cmds.
    //   Get previous then append for dcmd
    
    // DeltaConnection, DeltaLink operations to all use PatchLogInfo: 
    //   DeltaLink.getCurrentVersion

    // Write patch to tmp file, move into place to accept.
    //    Or verify in mem space then write when accepted. 
    
    // Abstract DeltaConnection - more tests, clean tests
    //   Rebuild directly rebuild by sync.
    
    // DeltaConnection : commit order
    //   Fake a "prepare"
    
    // Server side - check DSD name :"^[\w-_]+$"
    // Initial data : "version 0"
    // getInitialData -> stream of quads (as a patch?)
    // Shaded jar sever - check

    // XXX [INIT]
    // DLink : get URL for initial data. (file, 
    // DeltaConnection.initData : read URL. 
    //   Conneg for DPS.
    
    // Migrate PatchLogServer
    
    // URI design. For patch logs 
    //   http://server:1066/{WebAppCxt}/{shortName}/
    //                                 /{shortName}/init = "version 0" but dataset vs patch.
    //                                 /{shortName}/current = "version MaxInt"
    //                                 /{shortName}/patch/version
    //                                 /{shortName}/patch/id
    // Then delta is admin/control.

    // Log to update triple store
    // Log storage abstraction.

    // POST patch -> 201 Created + ** Location: **
    
    // Renames:
    //   Send patch => append patch
    //   Server PatchLog -> PatchLogStore
    //   DeltaConnection -> PatchLog, DeltaPatchLog, DataSource
    //     PatchLogConnection PatchLogConn
    
    // Tests:
    //   bad patch
    //   empty patch
    
    // Documentation
    //   Patch
    //   Protocol/delta
    // Version -> long?
    // Eliminate use of Location and just use Path.
    
    // Streaming patches.
    // Currently not because of DeltaLink.sendPatch takes a patch as argument. 
    
    //PatchLog - conflates "index" and "version" - acceptable?
    //          - revisit HistoryEntry - it keeps patches? LRU cache of patches.  
    
    // ------------------------------------------------------------------------
    // Document, "Design" protocol
    // -- History and parenthood.

    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    // -- Others
    // Extract polling support to DeltaClient.

    // DatasetGraphBuffering
    // GraphBuffering
    // BufferingTupleSet<X>
    
}