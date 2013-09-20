package gov.noaa.pmel.tmap.cleaner.jdo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Errors {
    private String filename = null;
    private List<String> messages = new ArrayList<String>();

    public void addMessage(String message) {
        messages.add(message);
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
                fo.close();
            } catch (FileNotFoundException e) {
                System.out.println("error message writing failed.");
               // ah well, no errors get written this time.
            }
        }
    }


}
