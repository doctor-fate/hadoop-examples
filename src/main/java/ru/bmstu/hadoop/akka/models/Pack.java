package ru.bmstu.hadoop.akka.models;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import ru.bmstu.hadoop.akka.messages.ExecutePackMessage;

import java.util.ArrayList;
import java.util.List;

public class Pack {
    private final List<Result> results = new ArrayList<>();

    public List<Result> getResults() {
        return results;
    }

    @JsonPropertyOrder({ "name", "expected", "result" })
    public static class Result {
        private final String name;
        private final String result, expected;

        public Result(ExecutePackMessage.Test test, String result) {
            this.name = test.name;
            this.result = result;
            this.expected = test.expected;
        }

        @SuppressWarnings("unused")
        public String getName() {
            return name;
        }

        @SuppressWarnings("unused")
        public Object getResult() {
            return result;
        }

        @SuppressWarnings("unused")
        public String getExpected() {
            return expected;
        }
    }
}