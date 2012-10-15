package gov.noaa.pmel.tmap.cleaner.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CrawlerOptions extends Options {
    /**
     * 
     */
    private static final long serialVersionUID = 8161908636390601749L;

    public CrawlerOptions() {
        Option root = new Option("r", "root", true, "The root or parent URL.");
        root.setRequired(true);
        addOption(root);
        Option exclude = new Option("x", "exclude", true, "Regular expression of URLs to exclude.");
        addOption(exclude);
        Option threads = new Option("t", "threads", true, "Number of threads to use in the thread pool.");
        addOption(threads);
        Option database = new Option("d", "database", true, "Name of the catalog datastore, usuall cc_yyyyMMdd.");
        addOption(database);
        Option threddsContext = new Option("c", "context", true, "The thredds context name e.g. 'geoide', the default is 'thredds'.");
        addOption(threddsContext);
        Option threddsServer = new Option("s", "server", true, "The URL of the server where the clean catalogs will be deployed. (Requried for cleaning)");
        addOption(threddsServer);
        Option url = new Option("u", "url", true, "URL of a sub-catalog to clean.");
        addOption(url);
        Option force = new Option("f", "force", false, "Download everything again (catalogs and/or netCDF data) regardless of the status of the previous crawl.");
        addOption(force);
        Option leaf = new Option("l", "leaf", true, "The OPeNDAP URL of a leaf node.");
        addOption(leaf);
    }
}
