package org.batfish.topology;

import static org.batfish.common.topology.TopologyUtil.computeLayer2Topology;

//import com.github.benmanes.caffeine.cache.Cache;
//import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.batfish.common.BatfishException;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.IBatfish;
import org.batfish.common.topology.GlobalBroadcastNoPointToPoint;
import org.batfish.common.topology.HybridL3Adjacencies;
import org.batfish.common.topology.IpOwners;
import org.batfish.common.topology.L3Adjacencies;
import org.batfish.common.topology.Layer1Topologies;
import org.batfish.common.topology.Layer1TopologiesFactory;
import org.batfish.common.topology.Layer1Topology;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.common.topology.TopologyUtil;
import org.batfish.common.topology.TunnelTopology;
import org.batfish.common.topology.broadcast.BroadcastL3Adjacencies;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.NetworkConfigurations;
import org.batfish.datamodel.Topology;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.datamodel.ipsec.IpsecTopology;
import org.batfish.datamodel.ospf.OspfTopology;
import org.batfish.datamodel.ospf.OspfTopologyUtils;
import org.batfish.datamodel.vxlan.VxlanTopology;
import org.batfish.datamodel.vxlan.VxlanTopologyUtils;
import org.batfish.storage.StorageProvider;

/**
 * A modification of TopologyProviderImpl, which instead of always getting configurations from Batfish,
 * accepts and uses externally specified configs, allowing generating topology from modified configs in iibdp.
 *
 * This implementation also abandons the use of caches, since configs is constantly changing,
 * requiring new topology very frequently.
 *
 *
 *
 * A weird thing exists:
 *
 * I tried keeping the cache, and just manually clearing them after IibdpPlugin used them to retrieve l3 interfaces,
 * and an empty config was generated to let IibdpEngine start with.
 *
 * I never cleared them in IibdpEngine ever after, letting them return the cached oldest version of topo every time,
 * namely, the initial empty topo with no links enabled.
 *
 * But the engine was still able to converge based on the desired interface enabling sequence
 * I just can't figure out why. How could you converge if topology never updated?
 *
 * The only explanation is that topology wasn't used as a reference during engine computation.
 * Instead, only node configs were used to capture newly established interfaces and links,
 * and compute IGP routes needed by BGP peering.
 *
 * But I'd still keep the topo updating along with the configs.
 * You never know what kinda weird bugs would pop up due to this inconsistency.
 */
@ParametersAreNonnullByDefault
public final class TopologyProviderCustomConfigs implements TopologyProvider {
    /** Create a new topology provider for a given instance of {@link IBatfish} */
    public TopologyProviderCustomConfigs(IBatfish batfish, StorageProvider storage) {
        _batfish = batfish;
        _storage = storage;
        _configs = null;
    }

    @Override
    public @Nonnull IpOwners getInitialIpOwners(NetworkSnapshot snapshot) {
        return computeInitialIpOwners(snapshot);
    }

    /**
     * Get IP owners based on a custom configs.
     * This is not declared in TopologyProvider.java, so downcast is needed.
     */
    public @Nonnull IpOwners getCustomIpOwners(NetworkSnapshot snapshot) {
        return computeCustomIpOwners(snapshot);
    }

    @Override
    public @Nonnull IpsecTopology getInitialIpsecTopology(NetworkSnapshot networkSnapshot) {
        return computeInitialIpsecTopology(networkSnapshot);
    }

    @Override
    public @Nonnull Topology getInitialLayer3Topology(NetworkSnapshot networkSnapshot) {
        return computeInitialLayer3Topology(networkSnapshot);
    }

    @Override
    public @Nonnull L3Adjacencies getInitialL3Adjacencies(NetworkSnapshot networkSnapshot) {
        return computeInitialL3Adjacencies(networkSnapshot);
    }

    @Override
    public @Nonnull OspfTopology getInitialOspfTopology(NetworkSnapshot networkSnapshot) {
        return computeInitialOspfTopology(networkSnapshot);
    }

    @Override
    public @Nonnull Layer1Topologies getLayer1Topologies(NetworkSnapshot networkSnapshot) {
        return createLayer1Topologies(networkSnapshot);
    }

    @Override
    public OspfTopology getOspfTopology(NetworkSnapshot networkSnapshot) {
        return computeOspfTopology(networkSnapshot);
    }

    @Override
    public @Nonnull Optional<Layer1Topology> getRawLayer1PhysicalTopology(
            NetworkSnapshot networkSnapshot) {
        return computeRawLayer1PhysicalTopology(networkSnapshot);
    }

    @Override
    public @Nonnull Topology getRawLayer3Topology(NetworkSnapshot networkSnapshot) {
        return computeRawLayer3Topology(networkSnapshot);
    }

    @Override
    public @Nonnull VxlanTopology getInitialVxlanTopology(NetworkSnapshot snapshot) {
        return computeInitialVxlanTopology(snapshot);
    }

    @Override
    public @Nonnull BgpTopology getBgpTopology(NetworkSnapshot snapshot) {
        try {
            return _storage.loadBgpTopology(snapshot);
        } catch (IOException e) {
            throw new BatfishException("Could not load BGP topology", e);
        }
    }

    @Override
    public @Nonnull Topology getLayer3Topology(NetworkSnapshot snapshot) {
        try {
            return _storage.loadLayer3Topology(snapshot);
        } catch (IOException e) {
            throw new BatfishException("Could not load layer-3 topology", e);
        }
    }

    @Override
    public @Nonnull L3Adjacencies getL3Adjacencies(@Nonnull NetworkSnapshot snapshot) {
        try {
            return _storage.loadL3Adjacencies(snapshot);
        } catch (IOException e) {
            throw new BatfishException("Could not load L3 Adjacencies", e);
        }
    }

    @Override
    public @Nonnull VxlanTopology getVxlanTopology(NetworkSnapshot snapshot) {
        try {
            return _storage.loadVxlanTopology(snapshot);
        } catch (IOException e) {
            throw new BatfishException("Could not load VXLAN topology", e);
        }
    }

    @Override
    public @Nonnull TunnelTopology getInitialTunnelTopology(NetworkSnapshot snapshot) {
        return computeInitialTunnelTopology(snapshot);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // PRIVATE IMPLEMENTATION
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private final IBatfish _batfish;
    private final StorageProvider _storage;
    private Map<String, Configuration> _configs;

    // NOTE: only the "raw" or "initial" versions of topologies are cached. This choice was made to
    // ease developer iteration on BDP: if the dataplane is re-generated (presumably, via a call to
    // generate_dataplane), the backend will not cache dataplane-derived topologies.

    private @Nonnull Map<String, Configuration> getConfigurations(NetworkSnapshot snapshot) {
        return _batfish
                .getProcessedConfigurations(snapshot)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Snapshot '" + snapshot + "' has not been parsed/serialized"));
    }

    /**
     * Accepts an externally defined configs. Later topology computation will use this configs.
     * This method is not declared in TopologyProvider.java, therefore you need to downcast before calling it.
     */
    public void setConfigurations(Map<String, Configuration> configs) {
        _configs = configs;
    }

    private @Nonnull IpOwners computeInitialIpOwners(NetworkSnapshot snapshot) {
        return new PreDataPlaneIpOwners(getConfigurations(snapshot), getInitialL3Adjacencies(snapshot));
    }

    private @Nonnull IpOwners computeCustomIpOwners(NetworkSnapshot snapshot) {
        assert _configs != null : "Custom config not set.";
        return new PreDataPlaneIpOwners(_configs, getInitialL3Adjacencies(snapshot));
    }

    private Optional<Layer1Topology> loadSynthesizedLayer1Topology(NetworkSnapshot networkSnapshot) {
        try {
            return _storage.loadSynthesizedLayer1Topology(networkSnapshot);
        } catch (IOException e) {
            throw new BatfishException("Could not load synthesized layer1 topology", e);
        }
    }

    private @Nonnull Layer1Topologies createLayer1Topologies(NetworkSnapshot networkSnapshot) {
        assert _configs != null : "Custom config not set.";
        return Layer1TopologiesFactory.create(
                getRawLayer1PhysicalTopology(networkSnapshot).orElse(Layer1Topology.EMPTY),
                loadSynthesizedLayer1Topology(networkSnapshot).orElse(Layer1Topology.EMPTY),
                _configs);
    }

    /** Computes {@link IpsecTopology} with edges that have compatible IPsec settings */
    private IpsecTopology computeInitialIpsecTopology(NetworkSnapshot networkSnapshot) {
        assert _configs != null : "Custom config not set.";
        return TopologyUtil.computeIpsecTopology(_configs);
    }

    private Topology computeInitialLayer3Topology(NetworkSnapshot networkSnapshot) {
        return TopologyUtil.computeLayer3Topology(
                getRawLayer3Topology(networkSnapshot), ImmutableSet.of());
    }

    private @Nonnull L3Adjacencies computeInitialL3Adjacencies(NetworkSnapshot networkSnapshot) {
        assert _configs != null : "Custom config not set.";
        Layer1Topologies l1 = getLayer1Topologies(networkSnapshot);
        if (L3Adjacencies.USE_NEW_METHOD) {
            return BroadcastL3Adjacencies.create(
                    l1, VxlanTopology.EMPTY, _configs);
        }
        if (l1.getCombinedL1().isEmpty()) {
            return GlobalBroadcastNoPointToPoint.instance();
        }

        Map<String, Configuration> configs = _configs;
        return HybridL3Adjacencies.create(
                l1, computeLayer2Topology(l1.getActiveLogicalL1(), VxlanTopology.EMPTY, configs), configs);
    }

    private @Nonnull Optional<Layer1Topology> computeRawLayer1PhysicalTopology(
            NetworkSnapshot networkSnapshot) {
        return Optional.ofNullable(
                _storage.loadLayer1Topology(networkSnapshot.getNetwork(), networkSnapshot.getSnapshot()));
    }

    private @Nonnull Topology computeRawLayer3Topology(NetworkSnapshot networkSnapshot) {
        assert _configs != null : "Custom config not set.";
        Map<String, Configuration> configurations = _configs;
        L3Adjacencies adjacencies = getInitialL3Adjacencies(networkSnapshot);
        return TopologyUtil.computeRawLayer3Topology(adjacencies, configurations);
    }

    private @Nonnull OspfTopology computeInitialOspfTopology(NetworkSnapshot snapshot) {
        assert _configs != null : "Custom config not set.";
        return OspfTopologyUtils.computeOspfTopology(
                NetworkConfigurations.of(_configs), getInitialLayer3Topology(snapshot));
    }

    private @Nonnull OspfTopology computeOspfTopology(NetworkSnapshot snapshot) {
        assert _configs != null : "Custom config not set.";
        return OspfTopologyUtils.computeOspfTopology(
                NetworkConfigurations.of(_configs), getLayer3Topology(snapshot));
    }

    private @Nonnull TunnelTopology computeInitialTunnelTopology(NetworkSnapshot snapshot) {
        assert _configs != null : "Custom config not set.";
        return TopologyUtil.computeInitialTunnelTopology(_configs);
    }

    private @Nonnull VxlanTopology computeInitialVxlanTopology(NetworkSnapshot snapshot) {
        assert _configs != null : "Custom config not set.";
        return VxlanTopologyUtils.computeInitialVxlanTopology(_configs);
    }
}
