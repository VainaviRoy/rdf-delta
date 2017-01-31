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

package org.seaborne.delta.server.http;

import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;

import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.link.DeltaLink;

/** fetch by "?id=" */
public class S_FetchId extends FetchBase {
    public S_FetchId(AtomicReference<DeltaLink> engine) {
        super(engine);
    }

    @Override
    protected Args getArgs(HttpServletRequest req) {
        Args a = Args.args(req);
        if ( a.dataset == null )
            throw new DeltaBadRequestException("No dataset specificed");
        if ( a.zone == null )
            Delta.DELTA_HTTP_LOG.warn("No Zone specified");
        if ( a.patchId == null && a.version == null )
            throw new DeltaBadRequestException("No patch id, no version");
        return a;
    }
    
    @Override
    protected String getOpName() {
        return "fetch";
    }
}