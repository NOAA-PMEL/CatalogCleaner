package gov.noaa.pmel.tmap.cleaner.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CrawlerOptions extends Options {
    /**
     * 
     */
    private static final long serialVersionUID = 8161908636390601749L;

    public CrawlerOptions() {
        Option root = new Option("r", "root", true, "The root URL for this tree crawl.");
        root.setRequired(true);
        addOption(root);
        Option exclude = new Option("x", "exclude", true, "Regular expression of URLs to exclude.");
        addOption(exclude);
        Option threads = new Option("t", "threads", true, "Number of threads to use in the thread pool.");
        addOption(threads);
        Option database = new Option("d", "database", true, "Name of the catalog datastore, usuall cc_yyyyMMdd.");
        addOption(database);
    }
}
