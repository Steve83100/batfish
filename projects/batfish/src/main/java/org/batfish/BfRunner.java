package org.batfish;

import static org.batfish.utils.BatfishUtils.NETWORKS_BASE;
import static org.batfish.utils.BatfishUtils.getBatfishFromTestrigText;

import java.nio.file.Path;
import java.util.Arrays;
//import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.BfConsts;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
//import org.batfish.datamodel.answers.AnswerElement;
//import org.batfish.datamodel.questions.Question;
import org.batfish.dataplane.ibdp.InterfaceModification;
import org.batfish.main.Batfish;
import org.batfish.utils.BatfishUtils;
import org.batfish.utils.ResultPrinter;

public class BfRunner {
  private static final Logger LOGGER = LogManager.getLogger(BfRunner.class);

  String networkName;

  Path networkInput;
  Path snapshotDir;

  Batfish batfish;

  public BfRunner(String networkName) {
    this.networkName = networkName;
    this.networkInput = NETWORKS_BASE.resolve("sourceFiles").resolve(networkName);
    LOGGER.info(String.format("network: %s, input dir: %s", networkName, networkInput));
  }

  public void parse() {
    Path configs = networkInput.resolve("configs");
    List<String> files =
        Arrays.stream(Objects.requireNonNull(configs.toFile().list()))
            .map(file -> configs.resolve(file).toString())
            .collect(Collectors.toList());
    BatfishUtils.Param param =
        new BatfishUtils.ParamBuilder()
            .setStorageBase(NETWORKS_BASE)
            .setNetworkName(networkName)
            .setSnapshotNameSuffix("bf")
            .setConfigurationPaths(files)
            .setLayer1Topology(configs.getParent().resolve(BfConsts.RELPATH_L1_TOPOLOGY_PATH))
//            .setDistributed(false)
            .build();
    Pair<Path, Batfish> pair = getBatfishFromTestrigText(param);
    snapshotDir = pair.getKey();
    batfish = pair.getValue();
  }

  public void printFinalDataPlane() {
    DataPlane dp = batfish.loadDataPlane(batfish.getSnapshot());
    ResultPrinter.printSnapshotFibs(dp, snapshotDir);
  }

  /**
   * Normally compute a data plane
   */
  public void computeDataPlane() {
    parse();
    assert batfish != null;
    batfish.computeDataPlane(batfish.getSnapshot());
    ResultPrinter.printSnapshotResult(batfish, snapshotDir, true, true, true, true, true, true, true);
  }

  /**
   * Uses IIBDP to compute a data plane incrementally by iteratively adding the specified interface modifications
   * @param intfMods Queue of interface modifications to be made
   */
  public void computeDataPlaneWithIntfMods(Queue<InterfaceModification> intfMods) {
    parse();
    assert batfish != null;
    batfish.computeDataPlaneWithIntfMods(batfish.getSnapshot(), intfMods);
    ResultPrinter.printSnapshotResult(batfish, snapshotDir, true, true, true, true, true, true, true);
  }

  /**
   * Parses, and uses topology provider to extract all interfaces (on one side of link) from configurations;
   */
  public List<InterfaceModification> parseAndLoadIntfs() {
    parse();
    assert batfish != null;
    return batfish.loadIntfs(batfish.getSnapshot());
  }

  /**
   * Uses IIBDP to compute the possibly different convergence states produced by enabling links in different orders;
   * Will try full permutation of all links by default;
   * TODO: Merge with parseAndLoadIntfs() to avoid parsing twice!!
   */
  public void computeDataPlaneWithLinkPerms() {
    parse();
    assert batfish != null;
    batfish.computeDataPlaneWithLinkPerms(batfish.getSnapshot());
    ResultPrinter.printSnapshotResultWithMultiDataPlane(batfish, snapshotDir, true, true, true, true, true, true, true);
  }

  /**
   * Uses IIBDP to compute the possibly different convergence states produced by resetting certain links;
   * Will try resetting all links once at a time by default
   */
  public void computeDataPlaneWithLinkResets() {
    parse();
    assert batfish != null;
    batfish.computeDataPlaneWithLinkResets(batfish.getSnapshot());
    ResultPrinter.printSnapshotResultWithMultiDataPlane(batfish, snapshotDir, true, true, true, true, true, true, true);
  }

  public Map<String, Configuration> getConfigurations() {
    return batfish.loadConfigurations(batfish.getSnapshot());
  }

  public Logger getLogger() {
    return LOGGER;
  }

  public static void main(String... args) {
    String network = "bgpWedgies/figure1/full";
    Queue<InterfaceModification> intfMods = new LinkedList<>();
    BfRunner test = new BfRunner(network);

//    intfMods.add(new InterfaceModification("r1", "FastEthernet0/0", true));
//    intfMods.add(new InterfaceModification("r1", "FastEthernet0/1", true));
    intfMods.add(new InterfaceModification("r1", "FastEthernet0/0", false));
//    intfMods.add(new InterfaceModification("r1", "FastEthernet0/1", false));
    test.computeDataPlaneWithIntfMods(intfMods);

//    test.computeDataPlaneWithLinkResets();
  }
}
