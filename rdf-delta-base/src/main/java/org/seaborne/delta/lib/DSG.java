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

package org.seaborne.delta.lib;

import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

/**
 * Functions on and about {@linkplain DatasetGraph}
 * @see DatasetGraphFactory
 */
public class DSG {

    /**
     * Parameter object to group the Node arguments treated as a pattern for deletion.
     */
    private static class DeletePattern {
        Node graphNode;
        Node subjectNode;
        Node predicateNode;
        Node objectNode;

        DeletePattern(Node g, Node s, Node p, Node o) {
            this.graphNode = g;
            this.subjectNode = s;
            this.predicateNode = p;
            this.objectNode = o;
        }
    }

    /**
     * Ensure the same graph object is returned by each of {@link DatasetGraph#getDefaultGraph()}
     * and {@link DatasetGraph#getGraph(Node)} each call.
     * This function does not "double wrap" a {@code DatasetGraph};
     * if the argument already provides "stable graphs" then the argument is returned unchanged.
     */
    public static DatasetGraph stableViewGraphs(DatasetGraph dsgBase) {
        if (dsgBase instanceof DatasetGraphStableGraphs)
            return dsgBase;

        // No harm wrapping one of these so let's not rely on the
        // implementation details of DatasetGraphMapLink.
        // if (dsgBase instanceof DatasetGraphMapLink)
        //     return dsgBase;

        DatasetGraph dsg1 = new DatasetGraphStableGraphs(dsgBase);
        return dsg1;
    }

    /**
     * Delete all quads matching the Node arguments treated as a pattern. This is done
     * without use of {@code Iterator.remove()}.
     *
     * Implemented is by repeated execution of {@code find(g, s, p, o)}, take a slice of the
     * results into an array and then delete the quads in the array from the dataset.
     * Exit the loop if the slice is short at which point the next {@code find(g, s, p, o)} would return no matches.
     */
    public static void deleteAny(DatasetGraph dsg, Node g, Node s, Node p, Node o) {
        deleteAny(dsg, new DeletePattern(g, s, p, o), DeleteBufferSize);
    }

    private static final int DeleteBufferSize = 1000;

    private static void deleteAny(DatasetGraph dsg, DeletePattern pattern, int sliceSize) {
        // Delete in slices rather than assume .remove() on the iterator is implemented.
        // We keep executing find(g, s, p, o) until we don't get a full slice.
        Quad[] buffer = new Quad[sliceSize];
        while (true) {
            Iterator<Quad> iter = dsg.find(pattern.graphNode, pattern.subjectNode, pattern.predicateNode, pattern.objectNode);
            // Get a slice
            int len = 0;
            for (; len < sliceSize; len++) {
                if (!iter.hasNext())
                    break;
                buffer[len] = iter.next();
            }
            // Delete them.
            for (int i = 0; i < len; i++) {
                dsg.delete(buffer[i]);
                buffer[i] = null;
            }
            // Finished?
            if (len < sliceSize)
                break;
        }
    }
}
