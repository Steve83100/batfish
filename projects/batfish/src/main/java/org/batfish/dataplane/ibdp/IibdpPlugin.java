package org.batfish.dataplane.ibdp;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import com.google.auto.service.AutoService;
import com.google.common.collect.Collections2;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
// import java.util.SortedSet;
import java.util.TreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.plugin.DataPlanePlugin;
import org.batfish.common.plugin.Plugin;
import org.batfish.common.topology.TopologyProvider;
import org.batfish.datamodel.BgpAdvertisement;
import org.batfish.datamodel.Configuration;
// import org.batfish.datamodel.Edge;
import org.batfish.datamodel.Topology;
import org.batfish.topology.TopologyProviderCustomConfigs;

/**
 * A new batfish plugin that registers the Incremental IBDP (iibdp) Engine. This new engine supports
 * an additional feature, which allows recomputation after config changes, enabling simulation of
 * how real-life config changes affect the already-converged network. It's why we call it
 * "incremental" IBDP. It incrementally computes a new converge state based on the old one.
 *
 * <p>For now, the new feature is implemented in a separate engine than the original ibdp so that it
 * remains untouched. Maybe in the future, it can be merged into the original ibdp.
 *
 * <p>Yes, there are two 'i's in the name "iibdp". I know that the 'i' in ibdp already stands for
 * incremental, but I was thinking, maybe this additional 'i' would make it sound...well...more
 * incremental? dunno
 */
@AutoService(Plugin.class)
public final class IibdpPlugin extends DataPlanePlugin {

  private static final Logger LOGGER = LogManager.getLogger(IibdpPlugin.class);

  public static final String PLUGIN_NAME = "iibdp";

  private IibdpEngine _engine;

  public IibdpPlugin() {}

  @Override
  public ComputeDataPlaneResult computeDataPlane(NetworkSnapshot snapshot) {
    LOGGER.info("Did not specify link changes. Will perform regular IBDP.");
    return computeDataPlaneWithIntfMods(
        snapshot, new LinkedList<InterfaceModification>()); // Compute DP with no changes
  }

  /**
   * Provides a sequence of interface modifications for IIBDP engine to compute Methods below are
   * not declared in DataPlanePlugin.java, therefore you need to downcast before calling them
   */
  public ComputeDataPlaneResult computeDataPlaneWithIntfMods(
      NetworkSnapshot snapshot, Queue<InterfaceModification> intfMods) {
    LOGGER.info("Iibdp Plugin: Loading information from snapshot...");
    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    TopologyProvider topologyProvider = _batfish.getTopologyProvider();

    LOGGER.info("Iibdp Plugin: Starting Iibdp engine...");
    ComputeDataPlaneResult answer =
        _engine.incrementalComputeDataPlane(
            snapshot, configurations, topologyProvider, externalAdverts, intfMods);
    _logger.infof("Incrementally generated data-plane for snapshot:%s", snapshot.getSnapshot());
    return answer;
  }

  /** Enumerates all link permutations, and calls IIBDP engine on these different permutations */
  public Map<String, ComputeDataPlaneResult> computeDataPlaneWithLinkPerms(
      NetworkSnapshot snapshot) {
    LOGGER.info("Iibdp Plugin: Permutations begin. Loading information from snapshot...");
    System.out.println("Iibdp Plugin: Permutations begin. Loading information from snapshot...");
    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    TopologyProvider topologyProvider = _batfish.getTopologyProvider();

    // Compute layer 3 topology out of configurations
    ((TopologyProviderCustomConfigs) topologyProvider).setConfigurations(configurations);
    Topology initialLayer3Topology = topologyProvider.getInitialLayer3Topology(snapshot);

    // SANITY CHECK: Look at layer 3 topology!
    //        SortedSet<Edge> l3Edges = initialLayer3Topology.sortedEdges();
    //        System.out.println("Within initialLayer3Topology in IibdpPlugin: ");
    //        for (Edge edge : l3Edges) {
    //            System.out.println("Edge tail node: " + edge.getNode1() + " intf: " +
    // edge.getInt1());
    //        }

    // Retrieve all layer 3 interfaces from topology. Only keep interface on one side of each edge
    Set<Set<String>> seenNodePairs = new HashSet<>();
    List<InterfaceModification> intfList =
        initialLayer3Topology.sortedEdges().stream()
            // With dual edges, only use tail side of one single edge for now, and filter out the
            // other edge
            .filter(
                edge -> {
                  Set<String> nodePair = Set.of(edge.getNode1(), edge.getNode2());
                  return seenNodePairs.add(nodePair);
                })
            .map(edge -> new InterfaceModification(edge.getNode1(), edge.getInt1(), true))
            .collect(toList());

    assert !(intfList.isEmpty())
        : "Iibdp Plugin: No interfaces detected. Cannot proceed computation.";

    LOGGER.info("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");
    System.out.println("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");

    // Generate all permutations of intfMods, store them in a collection of queues
    LOGGER.info("Iibdp Plugin: Generating all permutations of interfaces");
    Collection<Queue<InterfaceModification>> intfPerms =
        Collections2.transform(
            Collections2.permutations(intfList),
            LinkedList::new // Constructor reference to create LinkedList (which implements Queue)
            );
    LOGGER.info("Iibdp Plugin: Generated " + intfPerms.size() + " permutations");
    System.out.println("Iibdp Plugin: Generated " + intfPerms.size() + " permutations");

    Map<String, ComputeDataPlaneResult> hashToResult = new TreeMap<>();
    System.out.println(
        "Iibdp Plugin: Preparation finished. Starting full data plane computation...");
    System.out.println(
        "---------------------------------------------------------------------------");

    // Each queue stores a permutation of intfMods, use that order to enable target interfaces
    for (Queue<InterfaceModification> intfPerm : intfPerms) {

      // Edit configuration to disable all target interfaces at first
      for (InterfaceModification intfMod : intfList) {
        // Create disabling modification by modifying original intfMod
        InterfaceModification disableIntf =
            new InterfaceModification(intfMod._nodeName, intfMod._interfaceName, false);
        IibdpEngine.modifyInterfaceStatus(configurations, disableIntf, false);
      }

      LOGGER.info("Iibdp Plugin: Cold starting Iibdp engine on new permutation...");
      IbdpResultWithHash answer =
          _engine.incrementalComputeDataPlaneWithHash(
              snapshot, configurations, topologyProvider, externalAdverts, intfPerm);
      LOGGER.info("Iibdp Plugin: Generated data plane with hash: " + answer.getHash());
      //            System.out.println("Iibdp Plugin: Generated data plane with hash: " +
      // answer.getHash());

      // Keep dataplanes with distinct hash
      if (hashToResult.putIfAbsent(answer.getHash(), answer) == null) {
        System.out.println(
            "Iibdp Plugin: Discovered new data plane with hash: " + answer.getHash());
      }
    }
    System.out.println(
        "---------------------------------------------------------------------------");
    System.out.println(
        "Iibdp Plugin: Completed all data planes. Number of distinct results: "
            + hashToResult.size());
    return hashToResult;
  }

  /**
   * Calls IIBDP engine multiple times, each time applying one link reset This gurantees simulation
   * of real-life link reset outcomes, but is relatively slower
   */
  public Map<String, ComputeDataPlaneResult> computeDataPlaneWithIsolatedResets(
      NetworkSnapshot snapshot) {
    LOGGER.info("Iibdp Plugin: Isolated Resets begin. Loading information from snapshot...");
    System.out.println("Iibdp Plugin: Isolated resets begin. Loading information from snapshot...");
    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    TopologyProvider topologyProvider = _batfish.getTopologyProvider();

    // Compute layer 3 topology out of configurations
    ((TopologyProviderCustomConfigs) topologyProvider).setConfigurations(configurations);
    Topology initialLayer3Topology = topologyProvider.getInitialLayer3Topology(snapshot);

    // SANITY CHECK: Look at layer 3 topology!
    //        SortedSet<Edge> l3Edges = initialLayer3Topology.sortedEdges();
    //        System.out.println("Within initialLayer3Topology in IibdpPlugin: ");
    //        for (Edge edge : l3Edges) {
    //            System.out.println("Edge tail node: " + edge.getNode1() + " intf: " +
    // edge.getInt1());
    //        }

    // Retrieve all layer 3 interfaces from topology. Only keep interface on one side of each edge
    Set<Set<String>> seenNodePairs = new HashSet<>();
    List<InterfaceModification> intfList =
        initialLayer3Topology.sortedEdges().stream()
            // With dual edges, only use tail side of one single edge for now, and filter out the
            // other edge
            .filter(
                edge -> {
                  Set<String> nodePair = Set.of(edge.getNode1(), edge.getNode2());
                  return seenNodePairs.add(nodePair);
                })
            .map(edge -> new InterfaceModification(edge.getNode1(), edge.getInt1(), true))
            .collect(toList());

    assert !(intfList.isEmpty())
        : "Iibdp Plugin: No interfaces detected. Cannot proceed computation.";

    LOGGER.info("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");
    System.out.println("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");

    // Use LinkedHashMap to maintain occurence order
    Map<String, ComputeDataPlaneResult> hashToResult = new LinkedHashMap<>();

    System.out.println("Iibdp Plugin: Computing initial state without link resets...");
    IbdpResultWithHash initialAnswer =
        _engine.incrementalComputeDataPlaneWithHash(
            snapshot,
            configurations,
            topologyProvider,
            externalAdverts,
            new LinkedList<InterfaceModification>());
    hashToResult.put(initialAnswer.getHash(), initialAnswer);

    System.out.println(
        "Iibdp Plugin: Preparation finished. Starting full data plane computation...");
    System.out.println(
        "---------------------------------------------------------------------------");

    // Create a queue holding each link up one at a time
    Queue<InterfaceModification> intfUp = new LinkedList<>();

    for (InterfaceModification intfMod : intfList) {
      intfUp.add(intfMod);

      // Edit configuration to disable target interface first
      InterfaceModification disableIntf =
          new InterfaceModification(intfMod._nodeName, intfMod._interfaceName, false);
      IibdpEngine.modifyInterfaceStatus(configurations, disableIntf, false);

      LOGGER.info("Iibdp Plugin: Starting Iibdp engine on new link reset...");
      IbdpResultWithHash answer =
          _engine.incrementalComputeDataPlaneWithHash(
              snapshot, configurations, topologyProvider, externalAdverts, intfUp);
      LOGGER.info("Iibdp Plugin: Generated data plane with hash: " + answer.getHash());
      // System.out.println("Iibdp Plugin: Generated data plane with hash: " + answer.getHash());

      // Keep dataplanes with distinct hash
      if (hashToResult.putIfAbsent(answer.getHash(), answer) == null) {
        System.out.println(
            "Iibdp Plugin: Resetting "
                + intfMod._nodeName
                + " "
                + intfMod._interfaceName
                + " generated state with hash: "
                + answer.getHash());
      }
    }

    System.out.println(
        "---------------------------------------------------------------------------");
    System.out.println(
        "Iibdp Plugin: Completed all data planes. Number of distinct results: "
            + hashToResult.size());

    // Modify initial entry so that it gets labeled out
    for (Map.Entry<String, ComputeDataPlaneResult> initialEntry : hashToResult.entrySet()) {
      hashToResult.remove(initialEntry.getKey());
      hashToResult.put("_initial", initialEntry.getValue());
      break;
    }
    return hashToResult;
  }

  /**
   * Sequentially applies all link resets within one call to IIBDP engine, so computation is done
   * faster
   *
   * <p>However, this does not gurantee perfect simulation of real-life link resets, since Batfish
   * link-down is implemented from scratch by myself, and therefore not fully functional
   *
   * <p>It only supports removing existing routes from session downs, but does not save backup
   * routes, so it doesn't allow routes recovery after later enabling
   */
  public Map<String, ComputeDataPlaneResult> computeDataPlaneWithContinuousResets(
      NetworkSnapshot snapshot) {

    LOGGER.info("Iibdp Plugin: Continuous resets begin. Loading information from snapshot...");
    System.out.println(
        "Iibdp Plugin: Continuous resets begin. Loading information from snapshot...");

    Map<String, Configuration> configurations = _batfish.loadConfigurations(snapshot);
    Set<BgpAdvertisement> externalAdverts =
        _batfish.loadExternalBgpAnnouncements(snapshot, configurations);
    TopologyProvider topologyProvider = _batfish.getTopologyProvider();

    // Compute layer 3 topology out of configurations
    ((TopologyProviderCustomConfigs) topologyProvider).setConfigurations(configurations);
    Topology initialLayer3Topology = topologyProvider.getInitialLayer3Topology(snapshot);

    // SANITY CHECK: Look at layer 3 topology!
    //        SortedSet<Edge> l3Edges = initialLayer3Topology.sortedEdges();
    //        System.out.println("Within initialLayer3Topology in IibdpPlugin: ");
    //        for (Edge edge : l3Edges) {
    //            System.out.println("Edge tail node: " + edge.getNode1() + " intf: " +
    // edge.getInt1());
    //        }

    // Retrieve all layer 3 interfaces from topology. Only keep interface on one side of each edge
    Set<Set<String>> seenNodePairs = new HashSet<>();
    LinkedList<InterfaceModification> intfList =
        initialLayer3Topology.sortedEdges().stream()
            // With dual edges, only use tail side of one single edge for now, and filter out the
            // other edge
            .filter(
                edge -> {
                  Set<String> nodePair = Set.of(edge.getNode1(), edge.getNode2());
                  return seenNodePairs.add(nodePair);
                })
            .map(edge -> new InterfaceModification(edge.getNode1(), edge.getInt1(), true))
            .collect(toCollection(LinkedList::new));

    assert !(intfList.isEmpty())
        : "Iibdp Plugin: No interfaces detected. Cannot proceed computation.";

    LOGGER.info("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");
    System.out.println("Iibdp Plugin: Prepared " + intfList.size() + " interfaces");
    System.out.println("Iibdp Plugin: Preparation finished. Starting link reset trials...");
    System.out.println(
        "---------------------------------------------------------------------------");

    // For given interfaces, reset them one by one during computation, and detect inconsistencies
    Map<String, ComputeDataPlaneResult> hashToResult =
        _engine.incrementalComputeDataPlaneWithContinuousResets(
            snapshot, configurations, topologyProvider, externalAdverts, intfList);

    System.out.println(
        "---------------------------------------------------------------------------");
    System.out.println(
        "Iibdp Plugin: Completed all reset trials. Number of distinct results: "
            + hashToResult.size());

    // Modify initial entry so that it gets labeled out
    for (Map.Entry<String, ComputeDataPlaneResult> initialEntry : hashToResult.entrySet()) {
      hashToResult.remove(initialEntry.getKey());
      hashToResult.put("_initial", initialEntry.getValue());
      break;
    }

    return hashToResult;
  }

  @Override
  protected void dataPlanePluginInitialize() {
    _engine =
        new IibdpEngine(new IncrementalDataPlaneSettings(_batfish.getSettingsConfiguration()));
  }

  @Override
  public String getName() {
    return PLUGIN_NAME;
  }
}
