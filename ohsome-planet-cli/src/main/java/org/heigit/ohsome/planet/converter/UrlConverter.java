package org.heigit.ohsome.planet.converter;

import picocli.CommandLine;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class UrlConverter implements CommandLine.ITypeConverter<URL> {
    @Override
    public URL convert(String value) {
        try {
            var uri = new URI(value);
            return uri.toURL();
        } catch(IllegalArgumentException | URISyntaxException | MalformedURLException e) {
            throw new CommandLine.TypeConversionException("%s : %s".formatted(value, e.getMessage()));
        }
    }
}
