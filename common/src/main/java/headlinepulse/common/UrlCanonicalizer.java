package headlinepulse.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Reduces the many URLs that point at one article to a single form, so links can be matched and
 * counted per article.
 *
 * <p>The result is always {@code https://host/path?query}: the scheme becomes {@code https}; the host
 * is lowercased, with {@code www.}, {@code m.}, and {@code amp.} stripped and any userinfo or default
 * port dropped; AMP path forms, empty segments, and the trailing slash are removed; tracking and AMP
 * query parameters are dropped and the rest sorted; and the fragment is dropped. A bare domain becomes
 * {@code https://host} with no path. The path keeps its case, since servers may treat it as
 * case-sensitive.
 *
 * <p>Parsing is by hand rather than with {@link java.net.URI}, which rejects the unencoded spaces and
 * non-ASCII characters that real links often contain.
 */
public final class UrlCanonicalizer {
    private static final List<String> HOST_PREFIXES = List.of("www.", "m.", "amp.");

    /** Click and campaign trackers. Any parameter starting with {@code utm_} is dropped too. */
    private static final Set<String> TRACKING_PARAMS = Set.of(
            "fbclid", "gclid", "gclsrc", "dclid", "msclkid", "twclid", "igshid", "mibextid",
            "mc_cid", "mc_eid", "_ga", "_gl", "ref", "ref_src", "cmpid", "smid", "ocid", "sr_share", "_bhlid");

    /** The canonical form of {@code url}, or empty if it isn't an http(s) URL with a usable host. */
    public static Optional<String> canonicalize(String url) {
        String s = url.strip();
        int hash = s.indexOf('#');
        if (hash >= 0) {
            s = s.substring(0, hash);
        }

        int sep = s.indexOf("://");
        if (sep < 0) {
            return Optional.empty();
        }
        String scheme = s.substring(0, sep).toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            return Optional.empty();
        }

        String rest = s.substring(sep + 3);
        int authorityEnd = rest.length();
        for (char c : new char[] {'/', '?'}) {
            int i = rest.indexOf(c);
            if (i >= 0 && i < authorityEnd) {
                authorityEnd = i;
            }
        }
        String host = host(rest.substring(0, authorityEnd));
        if (host == null) {
            return Optional.empty();
        }

        String pathAndQuery = rest.substring(authorityEnd);
        int q = pathAndQuery.indexOf('?');
        String path = path(q < 0 ? pathAndQuery : pathAndQuery.substring(0, q));
        String query = q < 0 ? "" : query(pathAndQuery.substring(q + 1));

        return Optional.of("https://" + host + path + (query.isEmpty() ? "" : "?" + query));
    }

    /** The host of a URL that {@link #canonicalize} returned, without the port. */
    public static String hostOf(String canonicalUrl) {
        String rest = canonicalUrl.substring("https://".length());
        int end = rest.length();
        for (char c : new char[] {'/', '?', ':'}) {
            int i = rest.indexOf(c);
            if (i >= 0 && i < end) {
                end = i;
            }
        }
        return rest.substring(0, end);
    }

    /** Lowercase host with the prefixes stripped, plus any non-default port, or {@code null}. */
    private static String host(String authority) {
        String hostPort = authority.substring(authority.lastIndexOf('@') + 1);
        if (hostPort.startsWith("[")) {
            return null; // IPv6 literal: never a news site
        }
        String host = hostPort;
        String port = "";
        int colon = hostPort.indexOf(':');
        if (colon >= 0) {
            host = hostPort.substring(0, colon);
            port = hostPort.substring(colon + 1);
            if (port.equals("80") || port.equals("443")) {
                port = "";
            }
        }

        host = host.toLowerCase(Locale.ROOT);
        while (host.endsWith(".")) {
            host = host.substring(0, host.length() - 1);
        }
        boolean stripped;
        do {
            stripped = false;
            for (String prefix : HOST_PREFIXES) {
                // Keep at least a two-label domain: amp.dev stays amp.dev.
                if (host.startsWith(prefix) && host.indexOf('.', prefix.length()) > 0) {
                    host = host.substring(prefix.length());
                    stripped = true;
                }
            }
        } while (stripped);

        if (host.isEmpty() || host.indexOf('.') < 0 || host.contains(" ")) {
            return null;
        }
        return port.isEmpty() ? host : host + ":" + port;
    }

    /** Drops empty and {@code amp} segments and turns {@code story.amp.html} into {@code story.html}. */
    private static String path(String path) {
        List<String> segments = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (!segment.isEmpty() && !segment.equalsIgnoreCase("amp")) {
                segments.add(segment);
            }
        }
        if (segments.isEmpty()) {
            return "";
        }
        int last = segments.size() - 1;
        String name = segments.get(last);
        if (name.endsWith(".amp.html")) {
            segments.set(last, name.substring(0, name.length() - ".amp.html".length()) + ".html");
        } else if (name.endsWith(".amp")) {
            segments.set(last, name.substring(0, name.length() - ".amp".length()));
        }
        return "/" + String.join("/", segments);
    }

    /** The query without tracking or AMP parameters, sorted so parameter order doesn't matter. */
    private static String query(String query) {
        List<String> kept = new ArrayList<>();
        for (String param : query.split("&")) {
            int eq = param.indexOf('=');
            String name = (eq < 0 ? param : param.substring(0, eq)).toLowerCase(Locale.ROOT);
            String value = eq < 0 ? "" : param.substring(eq + 1);
            boolean amp = name.equals("amp") || (name.equals("outputtype") && value.equalsIgnoreCase("amp"));
            if (!name.isEmpty() && !amp && !name.startsWith("utm_") && !TRACKING_PARAMS.contains(name)) {
                kept.add(param);
            }
        }
        kept.sort(null);
        return String.join("&", kept);
    }

    private UrlCanonicalizer() {}
}
