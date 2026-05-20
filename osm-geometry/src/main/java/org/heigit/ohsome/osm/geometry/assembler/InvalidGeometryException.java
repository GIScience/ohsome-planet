package org.heigit.ohsome.osm.geometry.assembler;

class InvalidGeometryException extends RuntimeException {
    InvalidGeometryException(String message) {
        super(message, null, true, false); // suppress stack trace — used as control flow
    }
}
