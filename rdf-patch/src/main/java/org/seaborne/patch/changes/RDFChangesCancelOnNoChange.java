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

package org.seaborne.patch.changes;

import org.apache.jena.graph.Node ;
import org.seaborne.patch.RDFChanges ;

/**
 * Track whether an actual change has been made and direct the "commit" to
 * "noChangeCommit" if nothing changed.
 * 
 * See also elsewhere {@code RDFChangesHTTP}.  
 */
public abstract class RDFChangesCancelOnNoChange extends RDFChangesWrapper
{
    private boolean changeHappened = false ;
    public RDFChangesCancelOnNoChange(RDFChanges other) {
        super(other);
    }

    private void markChanged() {
        changeHappened = true;
    }
    
    @Override
    public void start() {}

    @Override
    public void finish() {}
    
    @Override
    public void header(String field, Node value) {
        markChanged();
        super.header(field, value);
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        markChanged();
        super.add(g, s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        markChanged();
        super.delete(g, s, p, o);
    }

    @Override
    public void addPrefix(Node graph, String prefix, String uriStr) {
        markChanged();
        super.addPrefix(graph, prefix, uriStr);
    }

    @Override
    public void deletePrefix(Node graph, String prefix) {
        markChanged();
        super.deletePrefix(graph, prefix);
    }

    @Override
    public void txnBegin() {
        changeHappened = false;
    }

    @Override
    public void txnCommit() {
        if ( changeHappened )
            super.txnCommit();
        else
            txnNoChangeCommit();
    }

    /** Called when commit called and there were no changes made. */ 
    public void txnNoChangeCommit() {
        super.txnCommit();
    }

    @Override
    public void txnAbort() {
        changeHappened = false ;
        super.txnAbort();
    }
    
}
