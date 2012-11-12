package gov.noaa.pmel.tmap.cleaner.xml;

import org.jdom2.Element;
import org.jdom2.filter.AbstractFilter;


public class DatasetNameFilter extends AbstractFilter {
    private String name;
    public DatasetNameFilter(String name) {
        this.name = name;
    }
    @Override
    public Object filter(Object obj) {
        if ( obj instanceof Element ) {
            Element e = (Element) obj;
            if ( e.getName().equals("dataset") ) {
                if ( e.getAttributeValue("name") != null &&  e.getAttributeValue("name").equals(name) ) return obj;
            }
        }
        return null;
    }
    
}
