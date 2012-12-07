package gov.noaa.pmel.tmap.cleaner.crawler;

import gov.noaa.pmel.tmap.cleaner.http.Proxy;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.util.Util;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.Parent;
import org.jdom2.filter.ElementFilter;

import thredds.catalog.InvAccess;
import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;

public class TreeCrawl implements Callable<TreeCrawlResult> {
    private static Proxy proxy = new Proxy();
    private static Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    private boolean force;
    private String url;
    private String parent;
    private Catalog catalog;
    private CatalogXML catalogXML;
    private PersistenceHelper helper;
    
    public TreeCrawl(PersistenceHelper helper, String parent, String url, boolean force) {   
        this.helper = helper;
        this.parent = parent;
        this.url = url;
        this.force = force;
    }
    @Override
    public TreeCrawlResult call() throws Exception {
       
        try {
            this.catalog = helper.getCatalog(parent, url);
            this.catalogXML = helper.getCatalogXML(url);
            if ( catalog == null ) {
                catalog = new Catalog();
                catalog.setParent(parent);
                catalog.setUrl(url);
                helper.save(catalog);
                catalogXML = null;
            }
            System.out.println("Downloading "+url+" in thread "+Thread.currentThread().getId());
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            try {
                proxy.executeGetMethodAndStreamResult(url, stream);
            } catch ( Exception e ) {
                System.err.println("Failed to read "+url+" in thread "+Thread.currentThread().getId());
                System.out.println("Commit transaction after read error in "+Thread.currentThread().getId());
                return new TreeCrawlResult(parent, url);
            }
            String xml = stream.toString();
            if ( catalogXML == null ) {
                catalogXML = new CatalogXML();
                catalogXML.setUrl(url);
                catalogXML.setXml(xml);
                System.out.println("Setting new XML for: " + url+" in "+Thread.currentThread().getId());
                helper.save(catalogXML);
            } else {
                String oldxml = catalogXML.getXml();
                if ( oldxml != null ) {
                    if ( oldxml.equals(xml) ) {
                        System.out.println("Using existing xml for: " + url+" in "+Thread.currentThread().getId());
                        if ( !force ) {
                            return new TreeCrawlResult(parent, url);
                        }
                    } else if ( !oldxml.equals(xml) ) {
                        System.out.println("Old XML not equal, using new for: " + url+" in "+Thread.currentThread().getId());
                        catalogXML.setXml(xml);
                    }
                } else  {
                    System.out.println("Old is null: " + url+" in "+Thread.currentThread().getId());
                    catalogXML.setXml(xml);
                }
            }
            Document doc = new Document();
            JDOMUtils.XML2JDOM(xml, doc);
            Element rootElement = doc.getRootElement();
            String version = rootElement.getAttributeValue("version");
            if(version != null){
                catalog.setVersion(version);
            }
            List<CatalogReference> refs = new ArrayList<CatalogReference>();
            refs = findCatalogRefs(url, rootElement, refs);
            catalog.setCatalogRefs(refs);
            List<LeafNodeReference> accessDatasets = new ArrayList<LeafNodeReference>();
            Iterator removeIt = doc.getDescendants(new ElementFilter("catalogRef"));
            Set<Parent> parents = new HashSet<Parent>();
            while ( removeIt.hasNext() ) {
                Element ref = (Element) removeIt.next();
                parents.add(ref.getParent());
            }
            for ( Iterator parentIt = parents.iterator(); parentIt.hasNext(); ) {
                Parent parent = (Parent) parentIt.next();
                parent.removeContent(new ElementFilter("catalogRef"));
            }
            catalog.setHasBestTimeSeries(false);
            accessDatasets = findAccessDatasets(url, doc, accessDatasets);
            catalog.setLeafNodes(accessDatasets);
            if ( catalog.hasBestTimeSeries() ) {
                catalog.setCatalogRefs(new ArrayList<CatalogReference>());
            }
            System.out.println("Commit transaction after normal processing in "+Thread.currentThread().getId());
            System.out.println("Finished with "+url+" in thread "+Thread.currentThread().getId());
            return new TreeCrawlResult(parent, url);
        } catch ( Exception e ) {
            System.err.println("Failed processing "+url+" with message "+e.getMessage()+" in thread "+Thread.currentThread().getId());
            return new TreeCrawlResult(parent, url);
        }
    }
    private List<CatalogReference> findCatalogRefs(String originalUrl, Element element, List<CatalogReference> catalogUrls) throws Exception {
        
        if (element.getName().equals("catalogRef")) {
            String suburl = element.getAttributeValue("href", xlink);
            String fullurl = Util.getUrl(originalUrl, suburl, "thredds");
            catalogUrls.add(new CatalogReference(suburl, fullurl));
        }
        List kids = element.getChildren();
        for (Iterator catIt = kids.iterator(); catIt.hasNext();) {
            Element kid = (Element) catIt.next();
            catalogUrls = findCatalogRefs(originalUrl, kid, catalogUrls);
        }
        return catalogUrls;
    }
    private List<LeafNodeReference> findAccessDatasets(String url, Document doc, List<LeafNodeReference> datasets) throws URISyntaxException {
        InvCatalogFactory catfactory = new InvCatalogFactory("default", false);
        String strippedDoc = JDOMUtils.toString(doc);
        InvCatalog thredds = (InvCatalog) catfactory.readXML(strippedDoc, new URI(url));
        List<InvDataset> rootInvDatasets = thredds.getDatasets();
        return findAccessDatasets(rootInvDatasets, datasets);
    }
    private List<LeafNodeReference> findAccessDatasets(List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for ( Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            if ( ds.hasAccess() ) {            
                InvAccess access = ds.getAccess(ServiceType.OPENDAP);
                if(access!=null){
                    String locationUrl = access.getStandardUrlName();
                    String urlPath = access.getUrlPath();
                    datasets.add(new LeafNodeReference(locationUrl, urlPath, DataCrawlStatus.NOT_STARTED));
                    if ( ds.getName().toLowerCase().contains("best time")) {
                        catalog.setHasBestTimeSeries(true);
                    }
                }

            } else if ( ds.hasNestedDatasets() ) {
                List<InvDataset> children = ds.getDatasets();
                findAccessDatasets(children, datasets);
            }
        }
        return datasets;
    }
    
}
