package gov.noaa.pmel.tmap.catalogcleaner.crawler;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.catalogcleaner.util.Util;
import gov.noaa.pmel.tmap.cleaner.http.Proxy;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.Parent;
import org.jdom2.filter.ElementFilter;
import org.jdom2.xpath.XPathHelper;


import thredds.catalog.InvAccess;
import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;

public class TreeCrawl implements Callable<TreeCrawlResult> {
    private static Proxy proxy = new Proxy();
    private static Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    private String url;
    private String parent;
    private Catalog catalog;
    private CatalogXML catalogXML;
    private static boolean bestTime = false;
    private static PersistenceHelper helper;
    public TreeCrawl(Properties properties, String parent, String url) {   
        this.parent = parent;
        this.url = url;
        JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
        PersistenceManager persistenceManager = pmf.getPersistenceManager();
        helper = new PersistenceHelper(persistenceManager);
        
    }
    @Override
    public TreeCrawlResult call() throws Exception {
        Transaction tx = helper.getTransaction();
        tx.begin();
        this.catalog = helper.getCatalog(parent, url);
        this.catalogXML = helper.getCatalogXML(url);
        if ( catalog == null ) {
            catalog = new Catalog();
            helper.save(catalog);
        }
        catalog.setParent(parent);
        catalog.setUrl(url);
        System.out.println("Downloading "+url+" in thread "+Thread.currentThread().getId());
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        proxy.executeGetMethodAndStreamResult(url, stream);
        String xml = stream.toString();
        if ( catalogXML == null ) {
            catalogXML = new CatalogXML();
            catalogXML.setUrl(url);
            helper.save(catalogXML);
        } else {
            if ( catalogXML != null ) {
                String oldxml = catalogXML.getXml();
                if ( oldxml.equals(xml) ) {
                    return new TreeCrawlResult(parent, url);
                }
            }
        }
        catalogXML.setXml(xml);
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
        accessDatasets = findAccessDatasets(url, doc, accessDatasets);
        catalog.setLeafNodes(accessDatasets);
        catalog.setHasBestTimeSeries(bestTime);
        if ( bestTime ) {
            catalog.setCatalogRefs(new ArrayList<CatalogReference>());
        }
        tx.commit();
        System.out.println("Finished with "+url+" in thread "+Thread.currentThread().getId());
        return new TreeCrawlResult(parent, url);
    }
    private static List<CatalogReference> findCatalogRefs(String originalUrl, Element element, List<CatalogReference> catalogUrls) throws Exception {
        
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
    private static List<LeafNodeReference> findAccessDatasets(String url, Document doc, List<LeafNodeReference> datasets) throws URISyntaxException {
        InvCatalogFactory catfactory = new InvCatalogFactory("default", false);
        String strippedDoc = JDOMUtils.toString(doc);
        InvCatalog thredds = (InvCatalog) catfactory.readXML(strippedDoc, new URI(url));
        List<InvDataset> rootInvDatasets = thredds.getDatasets();
        return findAccessDatasets(rootInvDatasets, datasets);
    }
    private static List<LeafNodeReference> findAccessDatasets(List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for ( Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            if ( ds.hasAccess() ) {            
                InvAccess access = ds.getAccess(ServiceType.OPENDAP);
                if(access!=null){
                    String locationUrl = access.getStandardUrlName();
                    String urlPath = access.getUrlPath();
                    datasets.add(new LeafNodeReference(locationUrl, urlPath, DataCrawlStatus.NOT_STARTED));
                    if ( ds.getName().toLowerCase().contains("best time")) bestTime = true;
                }

            } else if ( ds.hasNestedDatasets() ) {
                List<InvDataset> children = ds.getDatasets();
                findAccessDatasets(children, datasets);
            }
        }
        return datasets;
    }
    
}
