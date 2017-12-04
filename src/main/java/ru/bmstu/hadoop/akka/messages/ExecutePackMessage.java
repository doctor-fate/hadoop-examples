package ru.bmstu.hadoop.akka.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ExecutePackMessage {
    public final int id;
    public final String script;
    public final String function;
    public final List<Test> tests;

    @JsonCreator
    public ExecutePackMessage(@JsonProperty("id") int id, @JsonProperty("script") String script,
                              @JsonProperty("function") String function, @JsonProperty("tests") List<Test> tests) {
        this.id = id;
        this.script = script;
        this.function = function;
        this.tests = tests;
    }

    public static class Test {
        public final String name;
        public final Object[] parameters;
        public final String expected;

        @JsonCreator
        public Test(@JsonProperty("name") String name, @JsonProperty("expected") String expected, @JsonProperty("parameters") Object[] parameters) {
            this.name = name;
            this.expected = expected;
            this.parameters = parameters;
        }
    }
}