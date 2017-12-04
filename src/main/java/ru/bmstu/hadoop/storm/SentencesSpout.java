package ru.bmstu.hadoop.storm;

import com.google.common.io.Files;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

enum EState {
    SYNCING,
    WAITING,
    READING,
    POLLING,
    SLEEPING,
}

public class SentencesSpout extends BaseRichSpout {
    final static String POLL_DIR = "SentencesSpout_POLL_DIR";
    final static String PROCESSED_DIR = "SentencesSpout_PROCESSED_DIR";
    private Set<Object> send;
    private File current;
    private BufferedReader reader;
    private String processedDirectory, pollDirectory;
    private SpoutOutputCollector collector;
    private EState state = EState.POLLING;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        processedDirectory = (String) conf.get(PROCESSED_DIR);
        pollDirectory = (String) conf.get(POLL_DIR);
        send = new HashSet<>();
    }

    @Override
    public void nextTuple() {
        if (state == EState.SYNCING) {
            sync();
            state = EState.POLLING;
        }

        if (state == EState.POLLING) {
            openNextFile();
        }

        if (state == EState.READING) {
            sendNextLine();
        }

        if (state == EState.SLEEPING) {
            Utils.sleep(100);
            state = EState.POLLING;
        }
    }

    private void sendNextLine() {
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (line == null) {
            closeReader();
            state = EState.WAITING;
        } else {
            UUID id = UUID.randomUUID();
            collector.emit("sentences", new Values(line), id);
            send.add(id);
        }
    }

    private void closeReader() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        reader = null;
    }

    @Override
    public void ack(Object id) {
        send.remove(id);
        if (state == EState.WAITING && send.isEmpty()) {
            state = EState.SYNCING;
        }
    }

    private void sync() {
        collector.emit("sync", new Values());
        try {
            Files.move(current, new File(processedDirectory + '/' + current.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openNextFile() {
        current = null;
        for (File file : Files.fileTraverser().breadthFirst(new File(pollDirectory))) {
            if (file.isFile() && file.canRead()) {
                current = file;
            }
        }

        if (current == null) {
            state = EState.SLEEPING;
            return;
        }

        try {
            reader = new BufferedReader(new FileReader(current));
            state = EState.READING;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("sentences", new Fields("sentence"));
        declarer.declareStream("sync", new Fields());
    }
}
