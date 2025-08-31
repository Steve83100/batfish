package org.batfish.dataplane.ibdp;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.batfish.common.plugin.DataPlanePlugin.ComputeDataPlaneResult;
import org.batfish.common.topology.TopologyContainer;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.answers.DataPlaneAnswerElement;

/**
 * A specific type of {@link ComputeDataPlaneResult} returned by {@link IncrementalBdpEngine} which
 * includes a map of all {@link Node}s.
 *
 * <p>To be used in tests.
 */
@ParametersAreNonnullByDefault
final class IbdpResultWithHash extends ComputeDataPlaneResult {

    private final @Nonnull Map<String, Node> _nodes;

    private final @Nonnull String _hash;

    IbdpResultWithHash(
            DataPlaneAnswerElement answerElement,
            DataPlane dataPlane,
            TopologyContainer topologies,
            Map<String, Node> nodes,
            String hash) {
        super(answerElement, dataPlane, topologies);
        _nodes = nodes;
        _hash = hash;
    }

    @Nonnull
    Map<String, Node> getNodes() {
        return _nodes;
    }

    @Nonnull
    String getHash() {
        return _hash;
    }
}
