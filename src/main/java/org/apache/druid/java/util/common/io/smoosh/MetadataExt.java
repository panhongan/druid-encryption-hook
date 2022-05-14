package org.apache.druid.java.util.common.io.smoosh;

public class MetadataExt extends Metadata {

    private MetadataExt(int fileNum, int startOffset, int endOffset) {
        super(fileNum, startOffset, endOffset);
    }

    public static MetadataExt toMetadataExt(Object metadataObj) {
        Metadata metadata = (Metadata) metadataObj;
        return new MetadataExt(metadata.getFileNum(), metadata.getStartOffset(), metadata.getEndOffset());
    }
}
