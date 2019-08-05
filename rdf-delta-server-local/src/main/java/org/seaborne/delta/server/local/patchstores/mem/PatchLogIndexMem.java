/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.local.patchstores.mem;

import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchLogIndexBase2;

/** State control for a {@link PatchStore} */
public class PatchLogIndexMem extends PatchLogIndexBase2 implements PatchLogIndex {

    public PatchLogIndexMem() {
        super(new LogIndexMem(), null, null);
    }
//
//    @Override
//    protected Version genNextVersion() {
//        return getCurrentVersion().inc();
//    }
//
//
//    @Override
//    protected void savePrepare(Version version, Id patch, Id prev) {
//        // Check.
//    }
//
//    @Override
//    protected void saveCommit(Version version, Id patch, Id prev) {
//        // Inside the lock of PatchLogIndexBase
//        versionToId.put(version, patch);
//        patchHeaders.put(patch, new LogEntry(patch, version, prev));
//    }
//
//    @Override
//    protected Id fetchVersionToId(Version version) {
//        return versionToId.get(version);
//    }
//
//    @Override
//    protected LogEntry fetchPatchInfo(Id id) {
//        // Thread safe - LogEntry created at save time.
//        return patchHeaders.get(id);
//    }
//
//    @Override
//    public void release() {
//        versionToId.clear();
//        patchHeaders.clear();
//        saveCommit(Version.INIT, null, null);
//    }
//
//    @Override
//    public void delete() {
//        release();
//    }
//
//    @Override
//    public void runWithLock(Runnable action) {
//        synchronized(lock) {
//            action.run();
//        }
//    }
//
//    @Override
//    public <X> X runWithLockRtn(Supplier<X> action) {
//        synchronized(lock) {
//            return action.get();
//        }
//    }
//
//    @Override
//    public void syncVersionInfo() {}
}
