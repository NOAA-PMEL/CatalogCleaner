package gov.noaa.pmel.tmap.cleaner.jdo;

import gov.noaa.pmel.tmap.cleaner.crawler.Clean;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;


// For the time being, don't perisist this class... @PersistenceCapable
public class Rubric {
    
    //@Persistent
    //@Column(length=500)
    private String url;
    
    //@Persistent
    //@Column(length=500)
    private String parent;
    private String parentJson;
    
    private int leaves = 0;
    private int badLeaves = 0;
    private int aggregated = 0;
    private int totalServices = 6;
    private int services;
    private int fast;
    private int medium;
    private int slow;
    private List<String> children = new ArrayList<String>();
    
    public static long SLOW = 30000l;
    public static long MEDIUM = 2500l; 
    
    public List<String> getChildren() {
        return children;
    }
    public void setChildren(List<String> children) {
        this.children = children;
    }
    public void addChild(String url) {
        children.add(url);
    }
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    public String getParent() {
        return parent;
    }
    public void setParent(String parent) {
        this.parent = parent;
    }
    public String getParentJson() {
        return parentJson;
    }
    public void setParentJson(String parentJson) {
        this.parentJson = parentJson;
    }
    public int getLeaves() {
        return leaves;
    }
    public void setLeaves(int leaves) {
        this.leaves = leaves;
    }
    public int getBadLeaves() {
        return badLeaves;
    }
    public void setBadLeaves(int badLeaves) {
        this.badLeaves = badLeaves;
    }
    public int getAggregated() {
        return aggregated;
    }
    public void setAggregated(int aggregated) {
        this.aggregated = aggregated;
    }
    public void setServices(int services) {
        this.services = services;
    }
    public int getServices() {
        return this.services;
    }
    public int getTotalServices() {
        return totalServices;
    }
    public void setTotalServices(int totalServices) {
        this.totalServices = totalServices;
    }
    public int getFast() {
        return fast;
    }
    public void setFast(int fast) {
        this.fast = fast;
    }
    public void addFast(int add) {
        fast = fast + add;
    }
    public int getMedium() {
        return medium;
    }
    public void setMedium(int medium) {
        this.medium = medium;
    }
    public void addMedium(int add) {
        medium = medium + add;
    }
    public int getSlow() {
        return slow;
    }
    public void setSlow(int slow) {
        this.slow = slow;
    }
    public void addSlow(int add) {
        slow = slow + add;
    }
    public void addAggregated(int add) {
        aggregated = aggregated + add;
    }
    public void addLeaves(int add) {
        leaves = leaves + add;
    }
    public void addBadLeaves(int add) {
        badLeaves = badLeaves + add;
    }
    public void addServices(int add) {
        services = services + add;
    }
    public void write() throws FileNotFoundException, MalformedURLException {
        if ( url != null && parent != null ) {
            PrintWriter rout;
            String bf = Clean.getFileName(url).replace(".xml", "");
            if ( parent.equals(url) ) {
                rout = new PrintWriter(new File("CleanCatalogs"+File.separator+bf+"_rubric.json"));
            } else {
                rout = new PrintWriter(new File(Clean.getRubricFilePath(url)));
            }
            String json = new Gson().toJson(this);
            rout.println(json);
            rout.close();
        }
    }
//    public void print(PrintStream rout) {        
//        rout.println("Report for "+parent+" "+url);
//        rout.println("\tAggregated: "+aggregated);
//        rout.println("\tFiles: "+leaves);
//        if ( leaves-badLeaves > 0 ) {
//            rout.println("\t\tPercentage of files that were properly aggregated..."+(Double.valueOf(aggregated)/Double.valueOf(leaves-badLeaves))*100.d);
//        }
//        if ( leaves > 0 ) {
//            double good = 100.d - ((Double.valueOf(badLeaves)/Double.valueOf(leaves))*100.d);
//            rout.println("\t\tPercentage of files with good data..."+good);
//        }
//        rout.println("\t\tPercentage of UAF services active on the remote server... "+(Double.valueOf(services)/Double.valueOf(totalServices))*100.d);
//        if ( leaves-badLeaves > 0 ) {
//            rout.println("\t\tPercentage of files that returned the header in less than "+Double.valueOf(SLOW)/1000.d+" seconds..."+((Double.valueOf(fast)+Double.valueOf(medium))/Double.valueOf(leaves-badLeaves))*100.d);
//        }
       
//    }
    public static Rubric read(File rfile) throws IOException {
        FileReader reader = new FileReader(rfile);
        Rubric r = new Gson().fromJson(reader, Rubric.class);
        if ( reader != null ) {
            reader.close();
        }
        return r;
    }
}
