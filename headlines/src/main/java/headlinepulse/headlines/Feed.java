package headlinepulse.headlines;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/** An RSS or Atom feed to poll, from {@code config/feeds.yaml}. */
record Feed(String outlet, String url) {
    private record Config(List<Feed> feeds) {}

    static List<Feed> load(Path file) throws IOException {
        Config config = new ObjectMapper(new YAMLFactory()).readValue(file.toFile(), Config.class);
        if (config == null || config.feeds() == null || config.feeds().isEmpty()) {
            throw new IOException(file + " has no feeds");
        }
        for (Feed feed : config.feeds()) {
            if (feed.outlet() == null || feed.outlet().isBlank() || feed.url() == null || !feed.url().startsWith("http")) {
                throw new IOException(file + ": every feed needs an outlet and an http(s) url");
            }
        }
        return List.copyOf(config.feeds());
    }
}
