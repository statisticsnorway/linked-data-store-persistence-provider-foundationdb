package no.ssb.lds.core.persistence.foundationdb;

import org.testng.annotations.Test;

public class FoundationDBInitializerTest {

    @Test
    public void hexStringWithPrefix(){
        FoundationDBInitializer.hexToBytes("0x24");
    }

    @Test
    public void hexStringWithoutPrefix(){
        FoundationDBInitializer.hexToBytes("24");
    }
}
