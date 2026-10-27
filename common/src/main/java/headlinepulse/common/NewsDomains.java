package headlinepulse.common;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * The news outlets whose articles count as URL matches, loaded from {@code config/news-domains.yaml}.
 * A domain covers its subdomains too: {@code texastribune.org} matches {@code apps.texastribune.org}.
 */
public final class NewsDomains {
    private record Config(List<Outlet> outlets) {}

    private record Outlet(String name, List<String> domains) {}

    private final Map<String, String> outletByDomain;

    private NewsDomains(Map<String, String> outletByDomain) {
        this.outletByDomain = Map.copyOf(outletByDomain);
    }

    public static NewsDomains load(Path file) throws IOException {
        Config config = new ObjectMapper(new YAMLFactory()).readValue(file.toFile(), Config.class);
        if (config == null || config.outlets() == null) {
            throw new IOException(file + " has no outlets");
        }
        Map<String, String> byDomain = new HashMap<>();
        for (Outlet outlet : config.outlets()) {
            if (outlet.name() == null || outlet.domains() == null || outlet.domains().isEmpty()) {
                throw new IOException(file + ": every outlet needs a name and at least one domain");
            }
            for (String domain : outlet.domains()) {
                add(byDomain, domain, outlet.name());
            }
        }
        return new NewsDomains(byDomain);
    }

    /** For tests: domain → outlet name. */
    public static NewsDomains of(Map<String, String> outletByDomain) {
        Map<String, String> byDomain = new HashMap<>();
        outletByDomain.forEach((domain, outlet) -> add(byDomain, domain, outlet));
        return new NewsDomains(byDomain);
    }

    /** Rejects domains written in a form canonical hosts never take, such as {@code www.} or uppercase. */
    private static void add(Map<String, String> byDomain, String domain, String outlet) {
        String canonical = UrlCanonicalizer.canonicalize("https://" + domain).orElse(null);
        if (!("https://" + domain).equals(canonical)) {
            throw new IllegalArgumentException("Domain '" + domain + "' isn't in canonical form" +
                    (canonical == null ? "" : "; use '" + UrlCanonicalizer.hostOf(canonical) + "'"));
        }
        String previous = byDomain.putIfAbsent(domain, outlet);
        if (previous != null) {
            throw new IllegalArgumentException("Domain '" + domain + "' is listed twice");
        }
    }

    /** The outlet whose domain covers this canonical URL's host, if any. */
    public Optional<String> outlet(String canonicalUrl) {
        String host = UrlCanonicalizer.hostOf(canonicalUrl);
        while (true) {
            String outlet = outletByDomain.get(host);
            if (outlet != null) {
                return Optional.of(outlet);
            }
            int dot = host.indexOf('.');
            if (dot < 0) {
                return Optional.empty();
            }
            host = host.substring(dot + 1);
        }
    }

    public int size() {
        return outletByDomain.size();
    }
}
