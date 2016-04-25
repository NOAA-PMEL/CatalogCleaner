package gov.noaa.pmel.tmap.cleaner.jdo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Errors {
    private String filename = null;
    private List<String> messages = new ArrayList<String>();
    private List<String> skips = new ArrayList<String>();
    private static String SKIP_MESSAGE = "We configured the catalog cleaner to skip the following catalogs.  Most likely it was skipped because it contains un-aggregated data.";
    public void addMessage(String message) {
        messages.add(message);
    }
    public void addSkip(String url) {
        skips.add(url);
    }
    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    }
    public List<String> getMessages() {
        return messages;
    }
    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
    public List<String> getSkips() {
        return skips;
    }
    public void setSkips(List<String> skips) {
        this.skips = skips;
    }
    public void write() {
        if ( filename != null ) {
            File f = new File(filename);
            System.out.println("writing "+filename);
            try {
                PrintWriter fo = new PrintWriter(f);
                for (Iterator messIt = messages.iterator(); messIt.hasNext();) {
                    String mess = (String) messIt.next();
                    fo.println(mess);
                }
                if ( skips.size() > 0 ) {
                    fo.println(SKIP_MESSAGE);
                }
                for (Iterator skipIt = skips.iterator(); skipIt.hasNext();) {
                    String url = (String) skipIt.next();
                    fo.println(url);
                }
                fo.close();
            } catch (FileNotFoundException e) {
                System.out.println("error message writing failed.");
               // ah well, no errors get written this time.
            }
        }
    }
    public static Errors read(File efile) {
        Errors errors = new Errors();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(efile));
            String line = reader.readLine();
            boolean addSkips = false;
            while (line != null ) {
                if ( line.equals(SKIP_MESSAGE) ) {
                    addSkips = true;
                }
                if ( !addSkips ) {
                    errors.addMessage(line);
                } else {
                    if ( !line.equals(SKIP_MESSAGE) ) {
                        errors.addSkip(line);
                    }
                }
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return errors;
    }

}
