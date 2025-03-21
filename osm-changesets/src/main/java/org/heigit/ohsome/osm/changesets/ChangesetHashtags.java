package org.heigit.ohsome.osm.changesets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.regex.Pattern.compile;

public class ChangesetHashtags {
    public static final Pattern HASHTAG_PATTERN = compile("#[^\\u2000-\\u206F\\u2E00-\\u2E7F\\v\\h\\\\'!\"#$%()*,./:;<=>?@\\[\\]^`{|}~]+");

    public static List<String> hashTags(Map<String, String> tags) {
        return Stream.of("hashtags", "comment")
                .map(tags::get)
                .filter(Objects::nonNull)
                // https://github.com/openstreetmap/iD/blob/develop/modules/ui/commit.js#L523-L525
                // drop anything that looks like a URL - #4289
                .map(field -> field.replaceAll("http\\S*", ""))
                .map(HASHTAG_PATTERN::matcher)
                .flatMap(Matcher::results)
                .map(MatchResult::group)
                .map(group -> group.substring(1)) // remove #
                .filter(hashtag -> hashtag.length() >= 2 && !hashtag.matches("\\d+"))
                .distinct().sorted().toList();
    }
}
