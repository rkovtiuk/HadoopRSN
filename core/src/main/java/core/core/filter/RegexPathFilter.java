package core.core.filter;

import org.apache.hadoop.fs.Path;

public class RegexPathFilter {

    private final String regex;
    private final boolean include;

    public RegexPathFilter(String regex, boolean include) {
        this.regex = regex;
        this.include = include;
    }

    public RegexPathFilter(String regex) {
        this(regex, true);
    }

    public boolean accept(Path path){
        return (path.toString().matches(regex) ? include : !include);
    }
}
