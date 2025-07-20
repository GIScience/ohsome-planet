package org.heigit.ohsome.contributions.transformer;

public class TransformerException extends RuntimeException{
    public TransformerException(String s, Exception e) {
        super(s, e);
    }
}
