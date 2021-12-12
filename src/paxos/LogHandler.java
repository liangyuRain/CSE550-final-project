package paxos;

import lombok.extern.java.Log;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

@Log
public class LogHandler {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss SSS");

    public static final int LOG_FILE_SIZE_LIMIT = 1024 * 1024 * 1024;
    public static final int LOG_FILE_COUNT = 2;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT %1$tL] %5$s %n");
    }

    public static Level logLevel = Level.ALL;
    private final String prefix;

    public LogHandler(String prefix) {
        this.prefix = prefix;
    }

    public void addFile(String filename) throws IOException {
        FileHandler fh = new FileHandler(filename, LOG_FILE_SIZE_LIMIT, LOG_FILE_COUNT);
        fh.setFormatter(new SimpleFormatter());
        LOG.addHandler(fh);
    }

    public void log(Level level, String s) {
        if (level.intValue() >= logLevel.intValue()) {
            LOG.info(String.format("%s %s", prefix, s));
        }
    }

    public void log(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        pw.write(System.lineSeparator());
        pw.write(e.toString());
        pw.flush();
        this.log(Level.SEVERE, sw.toString());
        pw.close();
    }

    public LogHandler derivative(String prefix) {
        return new LogHandler(String.format("%s %s", this.prefix, prefix));
    }

}
