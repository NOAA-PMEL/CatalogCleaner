package gov.noaa.pmel.tmap.catalogcleaner.util;

import java.net.URI;
import java.net.URISyntaxException;

public class Util {
    public static String getUrl(String originalUrl, String stcatalogrefUri, String thredds) throws URISyntaxException{
        originalUrl = originalUrl.substring(0, originalUrl.lastIndexOf("/")+1);
        URI uri1 = new URI(originalUrl); 
        String base = uri1.getHost();
        if(uri1.getPort() > 0){
            base += ":" + uri1.getPort();
        }
        base += "/";
        URI catalogrefUri = new URI(stcatalogrefUri);
        if(!catalogrefUri.isAbsolute()){
            if(stcatalogrefUri.startsWith("/" + thredds + "/") && base.endsWith(thredds + "/")){
                stcatalogrefUri = stcatalogrefUri.substring(1 + thredds.length(), stcatalogrefUri.length());
            }
            if(stcatalogrefUri.startsWith("/")){
                URI remoteURI = new URI(base);
                catalogrefUri = new URI(remoteURI.toString() + stcatalogrefUri.replaceFirst("/", ""));
            }
            else{
                //URI url = new URI(rawCatalogref.getPrivateUrl().getValue());
                if(stcatalogrefUri.startsWith("./"))
                    stcatalogrefUri = stcatalogrefUri.substring(2, stcatalogrefUri.length());
                catalogrefUri = new URI(originalUrl + stcatalogrefUri);
            }
        }
        String url = catalogrefUri.toString();
        if(!url.startsWith("http://"))
            url = "http://" + url;

        return url;
    }
}
