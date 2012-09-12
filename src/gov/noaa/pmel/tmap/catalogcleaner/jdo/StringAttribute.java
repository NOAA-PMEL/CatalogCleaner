package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
@PersistenceCapable
public class StringAttribute {
    @Persistent
    String name;
    @Persistent
    List<String> value;
    public StringAttribute(String name, List<String> value) {
        super();
        this.name = name;
        this.value = value;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<String> getValue() {
        return value;
    }
    public void setValue(List<String> value) {
        this.value = value;
    }
    

}
