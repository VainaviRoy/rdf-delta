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

package org.seaborne.delta;

import java.util.UUID ;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.shared.uuid.UUIDFactory ;
import org.apache.jena.shared.uuid.UUID_V4_Gen;

/**
 * Move to rdf-patch?
 */
public final class Id {
    private static final String schemeUuid = "uuid:" ;
    private static final String schemeUrnUuid = "urn:uuid:" ;
    private static final String SCHEME = "id:";

    public static Id create() {
        return new Id(genUUID()) ;
    }
    
    public static Id fromNode(Node node) {
        if ( node == null )
            return null ;
        
        String s = null ;
        
        if ( node.isURI() )
            s = node.getURI() ;
        else if ( Util.isSimpleString(node) )
            s = node.getLiteralLexicalForm() ;
        
        if ( s == null )
            throw new IllegalArgumentException("Id input is not a URI or a string") ;
        return fromString$(s) ;
    } 

    public static Id fromUUID(UUID uuid) { return new Id(uuid) ; } 
    
    private static Id fromString$(String str) {
        if ( str.startsWith(schemeUuid) )
            str = str.substring(schemeUuid.length()) ;
        else if ( str.startsWith(schemeUrnUuid) )
            str = str.substring(schemeUrnUuid.length()) ;
        return fromString(str) ;
    }
    
    public static Id fromString(String str) {
        if ( str.startsWith(SCHEME) )
            str = str.substring(SCHEME.length());
        try {
            UUID uuid = UUID.fromString(str) ;
            return new Id(uuid) ; 
        } catch (IllegalArgumentException ex) {
            return new Id(str) ;
        }
    }

    // Version 1 are guessable.
    // Version 4 are not.
    private static UUIDFactory uuidFactory = new UUID_V4_Gen() ;

    /** {@link UUID}s are used to UUIDentify many things in Delta - the RDF Dataset being managed,
     * the patches applied (the UUID naming forms the history), registrations and channels,
     * amongst other things.
     */
    public static UUID genUUID() { return uuidFactory.generate().asUUID() ; }

    private final UUID uuid ;
    private final String string ;

    private Id(UUID id) {
        uuid = id ;
        string = null ;
    }

    private Id(String id) {
        uuid = null ;
        string = id ;
    }

    /** Suitable for putting into an HTTP request query string. */ 
    public String asParam() {
        if ( uuid != null ) 
            return uuid.toString() ;
        return string ;
    }
    
    /** Without any adornment */ 
    public String asPlainString() {
        if ( uuid != null ) 
            return uuid.toString() ;
        if ( string != null )
            return string ;
        throw new InternalErrorException("Id has null UUID and string");
    }

    /** With "schema" */ 
    public String asString() {
        if ( uuid != null ) 
            return SCHEME+uuid.toString() ;
        if ( string != null )
            return string ;
        throw new InternalErrorException("Id has null UUID and string");
    }

    /** Convert to a Node, URI or plain string. */ 
    public Node asNode() {
        if ( uuid != null ) 
            return NodeFactory.createURI(schemeUuid+uuid.toString());
        return NodeFactory.createLiteral(string);
    }

    // Not needed.
//    /** Suitable for using as a JSON string. Note this is encoded (if necessary).
//     * {@link JsonBuilder} applies encoding and this method is not needed to build a JSON object. */ 
//    public String asJsonString() {
//        return asPlainString();
//    }
    
    @Override
    public String toString() {
        if ( uuid != null ) 
            return SCHEME+uuid.toString() ;
        return SCHEME+"\""+string+"\"" ;
    }

    @Override
    public int hashCode() {
        final int prime = 31 ;
        int result = 1 ;
        result = prime * result + ((string == null) ? 0 : string.hashCode()) ;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode()) ;
        return result ;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true ;
        if ( obj == null )
            return false ;
        if ( getClass() != obj.getClass() )
            return false ;
        Id other = (Id)obj ;
        if ( string == null ) {
            if ( other.string != null )
                return false ;
        } else if ( !string.equals(other.string) )
            return false ;
        if ( uuid == null ) {
            if ( other.uuid != null )
                return false ;
        } else if ( !uuid.equals(other.uuid) )
            return false ;
        return true ;
    } 
}
