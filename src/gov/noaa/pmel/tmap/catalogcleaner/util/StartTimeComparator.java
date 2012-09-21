package gov.noaa.pmel.tmap.catalogcleaner.util;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.TimeAxis;

import java.util.Comparator;
import java.util.List;

/**
 * Compares start times in the first varaible in each dataset.  Lots of assumptions about the fact that the datasets already have the same "aggregation signature".
 * @author rhs
 *
 */
public class StartTimeComparator implements Comparator<Leaf> {

    @Override
    public int compare(Leaf o1, Leaf o2) {
        LeafDataset d1 = o1.getLeafDataset();
        LeafDataset d2 = o2.getLeafDataset();
        List<NetCDFVariable> variablesO1 = d1.getVariables();
        List<NetCDFVariable> variablesO2 = d2.getVariables();
        if ( variablesO1 == null || variablesO1.size() <= 0 ) return -1;
        if ( variablesO2 == null || variablesO1.size() <= 0 ) return 1;
        NetCDFVariable v1 = variablesO1.get(0);
        NetCDFVariable v2 = variablesO2.get(0);
        TimeAxis t1 = v1.getTimeAxis();
        TimeAxis t2 = v2.getTimeAxis();
        if ( t1 == null ) return -1;
        if ( t2 == null ) return 1;
        double startTime1 = t1.getMinValue();
        double startTime2 = t2.getMinValue();
        if ( startTime1 < startTime2 ) {
            return -1;
        } else if ( startTime1 > startTime2 ) {
            return 1;
        } else {
            return 0;
        }
    }

}
