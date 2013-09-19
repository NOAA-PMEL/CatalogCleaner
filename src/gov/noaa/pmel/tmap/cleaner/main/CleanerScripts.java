package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.joda.time.DateTime;

public class CleanerScripts {

    protected static String root;
    protected static String threddsContext;
    protected static String threddsServer;
    protected static String threddsServerName;
    protected static List<String> exclude = new ArrayList<String>();
    protected static List<String> excludeCatalog = new ArrayList<String>();
    protected static Map<String, List<String>> excludeDataset = new HashMap<String, List<String>>();
    protected static ExecutorService pool;
    protected static CompletionService<String> completionPool;
    protected static String database;
    protected static String url;
    protected static boolean varcheck;
    protected static boolean force;
    protected static boolean full;
    protected static JDOPersistenceManagerFactory pmf;
    protected static Properties properties;
    protected static CrawlerOptions crawlerOptions = new CrawlerOptions();
    protected static CommandLine cl;
    protected static String[] x;
    
    protected static Connection conn = null;
    protected static Statement statement = null;
    
    protected static String workingDir;
    
    public static void main(String[] args) {

        try {
            
            init(false, args);
            workingDir = new File(".").getAbsolutePath();
            workingDir = workingDir.substring(0, workingDir.length()-2);
            if ( !workingDir.endsWith(File.separator)) {
                workingDir = workingDir+File.separator;
            }
            System.out.println("Working dir: "+workingDir);
            StringBuilder xlist = new StringBuilder();
            if ( x != null ) {                
                for (int i = 0; i < x.length; i++) {
                    xlist.append(" -x "+x[i]);
                }
            }
            List<String> skipped = new ArrayList<String>();
            statement.execute("SELECT * from catalog where parent=\""+root+"\"");
            ResultSet rootResults = statement.getResultSet();
            while ( rootResults.next() ) {
                String caturl = rootResults.getString(rootResults.findColumn("url" ));
                URL cu = new URL(caturl);
                String host = cu.getHost();
                File hostDir = new File("CleanScripts"+File.separator+host);
                int index = 0;
                while ( hostDir.exists() && index < 10 ) {
                    hostDir =  new File("CleanScripts"+File.separator+host+index);
                }
                hostDir.mkdirs();
                
                System.out.println(host);
                Statement substatement = conn.createStatement();
                substatement.execute("SELECT * from catalog where url like \"%"+host+"%\"");
                ResultSet subresults = substatement.getResultSet();
                
                
                File cleantop = new File(hostDir, "cleantop.sh");                
                PrintWriter cleanWriterTop = new PrintWriter(cleantop);
                File datatop = new File(hostDir, "datatop.sh");
                PrintWriter dataWriterTop = new PrintWriter(datatop);
                File dumptop = new File(hostDir, "dumptop.sh");
                PrintWriter dumpWriterTop = new PrintWriter(dumptop);
                File reporttop = new File(hostDir, "reporttop.sh");
                PrintWriter reportTopWriter = new PrintWriter(reporttop);

                dataWriterTop.println(workingDir+"bin"+File.separator+"datacrawler.sh -f"+" -s "+threddsServer+" -c "+threddsContext+" -r "+root+" -u "+caturl+xlist.toString()+" -d "+database+" 2>&1 | tee "+hostDir+"/data-`date +%Y-%m-%d_%H_%M`.log");
                dumpWriterTop.println(workingDir+"bin"+File.separator+"dump.sh"+" -s "+threddsServer+" -c "+threddsContext+" -r "+root+" -u "+caturl+xlist.toString()+" -d "+database+" 2>&1 | tee "+hostDir+"/dump-`date +%Y-%m-%d_%H_%M`.log");
                cleanWriterTop.println(workingDir+"bin"+File.separator+"clean.sh"+" -s "+threddsServer+" -c "+threddsContext+" -r "+root+" -u "+caturl+xlist.toString()+" -d "+database+" 2>&1 | tee "+hostDir+"/clean-`date +%Y-%m-%d_%H_%M`.log");
                reportTopWriter.println(workingDir+"bin"+File.separator+"report.sh"+" -s "+threddsServer+" -c "+threddsContext+" -r "+root+" -u "+caturl+xlist.toString()+" -d "+database+" 2>&1 | tee "+hostDir+"/report-`date +%Y-%m-%d_%H_%M`.log");

                dataWriterTop.close();
                dumpWriterTop.close();
                cleanWriterTop.close();
                reportTopWriter.close();
                cleantop.setExecutable(true, true);
                datatop.setExecutable(true, true);
                dumptop.setExecutable(true, true);
                reporttop.setExecutable(true, true);
                


//                
//                File clean = new File(hostDir, "clean.sh");
//                clean.setExecutable(true, true);
//                PrintWriter cleanWriter = new PrintWriter(clean);
//                File data = new File(hostDir, "data.sh");
//                data.setExecutable(true, true);
//                PrintWriter dataWriter = new PrintWriter(data);
//                File dump = new File(hostDir, "dump.sh");
//                dump.setExecutable(true, true);
//                PrintWriter dumpWriter = new PrintWriter(dump);
//                cleanWriter.println("#!/bin/sh");
//                dataWriter.println("#!/bin/sh");
//                dumpWriter.println("#!/bin/sh");
//                
//                while (subresults.next()) {
//                    String suburl = subresults.getString(subresults.findColumn("url"));
//                    boolean xclud = false;
//                    for ( int i = 0; i < exclude.size(); i++ ) {
//                        if ( Pattern.matches(exclude.get(i), suburl)) {
//                            xclud = true;
//                        }
//                    }
//                    if ( xclud || excludeCatalog.contains(suburl) || suburl.equals(caturl) ) {
//                        skipped.add(suburl);
//                    } else {
//                        String endargs = " -s "+threddsServer+" -c "+threddsContext+" -r "+caturl+" -u "+suburl+xlist.toString()+" -d "+database+" 2>&1 | tee ";
//                        String endargs2 = "`date +%Y-%m-%d_%H_%M`.log";
//                        dataWriter.println(workingDir+File.separator+"datacrawler.sh -f"+endargs+"data-"+endargs2);
//                        dumpWriter.println(workingDir+File.separator+"dump.sh"+endargs+"dump-"+endargs2);
//                        cleanWriter.println(workingDir+File.separator+"clean.sh"+endargs+"clean-"+endargs2);
//                    }
//                }
//                dumpWriter.close();
//                dataWriter.close();
//                cleanWriter.close();
//                subresults.close();
            }

            for (Iterator skIt = skipped.iterator(); skIt.hasNext();) {
                String skipped_url = (String) skIt.next();
                System.out.println("Skipped: "+skipped_url);
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JDOMException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
    public CleanerScripts() {
        super();
    }  
    public static void init(boolean create, String[] args) throws FileNotFoundException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, JDOMException{
        CommandLineParser parser = new GnuParser();
       
        int width = 80;
        try {

            URL skipFile = ClassLoader.getSystemResource("skip.xml");
            Document skipdoc = new Document();
            JDOMUtils.XML2JDOM(new File(skipFile.getFile()), skipdoc);

            Element skip = skipdoc.getRootElement();
            List<Element> regexEls = skip.getChildren("regex");
            for ( Iterator rIt = regexEls.iterator(); rIt.hasNext(); ) {
                Element regex = (Element) rIt.next();
                String value = regex.getAttributeValue("value");
                if ( value != null ) {
                    exclude.add(value);
                }
            }
            List<Element> catalogEls = skip.getChildren("catalog");
            for ( Iterator catIt = catalogEls.iterator(); catIt.hasNext(); ) {
                Element catE = (Element) catIt.next();
                String url = catE.getAttributeValue("url");
                if ( url != null ) {
                    if ( catE.getChildren("dataset").size() == 0 ) {
                        excludeCatalog.add(url);
                    } else {
                        List<Element> dsE = catE.getChildren("dataset");
                        List<String> exds = new ArrayList<String>();
                        excludeDataset.put(url, exds);
                        for ( Iterator dsIt = dsE.iterator(); dsIt.hasNext(); ) {
                            Element dataset = (Element) dsIt.next();
                            String name = dataset.getAttributeValue("name");
                            if ( name != null ) {
                                exds.add(name);
                            }
                        }
                    }
                }
            }

            cl = parser.parse(crawlerOptions, args);
            full = cl.hasOption("a");
            root = cl.getOptionValue("r");
            url = cl.getOptionValue("u");
            threddsContext = cl.getOptionValue("c");
            threddsServer = cl.getOptionValue("s");
            x = cl.getOptionValues("x");
            if ( x != null ) {
                for ( int i = 0; i < x.length; i++ ) {
                    exclude.add(x[i]);
                }
            }
            if ( threddsContext == null ) {
                threddsContext = "thredds";
            }

            String t = cl.getOptionValue("t");
            int threads = 1;
            try {
                if ( t != null )
                    threads = Integer.valueOf(t);
            } catch ( NumberFormatException e ) {
                // S'ok.  Use the default.
            }
            varcheck = cl.hasOption("v");
            force = cl.hasOption("f");
            pool = Executors.newFixedThreadPool(threads);
            completionPool = new ExecutorCompletionService<String>(pool);

            database = cl.getOptionValue("d");
            String tag = DateTime.now().toString("yyyyMMdd");
            if ( database == null ) {
                database = "cc_"+tag;
            }
            properties = new Properties() ;
            URL propertiesURL =  ClassLoader.getSystemResource("datanucleus.properties");
            properties.load(new FileInputStream(new File(propertiesURL.getFile())));
            String connectionURL = (String) properties.get("datanucleus.ConnectionURL");


            if ( connectionURL.contains("database") ) {
                connectionURL = connectionURL.replace("database", database);
            } else {
                System.err.println("The conenctionURL string should use the name \"database\" which will be substituted for from the command line arguments." );
                System.exit(-1);
            }
            String connectionUser = properties.getProperty("datanucleus.ConnectionUserName");
            String connectionPW = properties.getProperty("datanucleus.ConnectionPassword");
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            conn = DriverManager.getConnection(connectionURL+"?user="+connectionUser+"&password="+connectionPW); 
            statement=conn.createStatement();
            

        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("Cleaner", crawlerOptions, true);
            System.exit(-1);

        } 
    }
}