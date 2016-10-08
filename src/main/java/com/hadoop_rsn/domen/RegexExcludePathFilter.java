package com.hadoop_rsn.domen;

import org.apache.hadoop.fs.Path;

public class RegexExcludePathFilter {

    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    public boolean accept(Path path){
        return !path.toString().matches(regex);
    }
}
