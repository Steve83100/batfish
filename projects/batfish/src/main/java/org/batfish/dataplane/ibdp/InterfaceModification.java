package org.batfish.dataplane.ibdp;

/**
 * A specific modification to be made to interfaces.
 */
public final class InterfaceModification {

    public String _nodeName;
    public String _interfaceName;
    public boolean _shouldActivate;

    public InterfaceModification(String nodeName, String interfaceName, boolean shouldActivate){
        _nodeName = nodeName;
        _interfaceName = interfaceName;
        _shouldActivate = shouldActivate;
    }
}