package gov.noaa.pmel.tmap.cleaner.util;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;

/**
 * A temporary internal class that can hold both a LeafNodeReference and a LeafDataset.
 * @author rhs
 *
 */
public class Leaf {
    private LeafNodeReference leafNodeReference;
    private LeafDataset leafDataset;
    public Leaf(LeafNodeReference leafNodeReference, LeafDataset leafDataset) {
        super();
        this.leafNodeReference = leafNodeReference;
        this.leafDataset = leafDataset;
    }
    public LeafNodeReference getLeafNodeReference() {
        return leafNodeReference;
    }
    public void setLeafNodeReference(LeafNodeReference leafNodeReference) {
        this.leafNodeReference = leafNodeReference;
    }
    public LeafDataset getLeafDataset() {
        return leafDataset;
    }
    public void setLeafDataset(LeafDataset leafDataset) {
        this.leafDataset = leafDataset;
    }
}
