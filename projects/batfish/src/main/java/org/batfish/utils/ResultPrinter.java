package org.batfish.utils;

import static org.batfish.utils.StorageUtils.getBufferedWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Table;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.batfish.common.util.BatfishObjectMapper;
import org.batfish.datamodel.AbstractRoute;
import org.batfish.datamodel.AbstractRouteDecorator;
import org.batfish.datamodel.Bgpv4Route;
import org.batfish.datamodel.ConcreteInterfaceAddress;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.Fib;
import org.batfish.datamodel.FibEntry;
import org.batfish.datamodel.FinalMainRib;
import org.batfish.datamodel.Interface;
import org.batfish.datamodel.StaticRoute;
import org.batfish.datamodel.Topology;
import org.batfish.datamodel.Vrf;
import org.batfish.datamodel.bgp.BgpTopology;
import org.batfish.dataplane.ibdp.VirtualRouter;
// import org.batfish.dataplane.ibdp.VirtualRouterWrapper;
import org.batfish.main.Batfish;

public class ResultPrinter {
  private static final Logger LOGGER = LogManager.getLogger(ResultPrinter.class);

  public static void printSnapshotResult(
      Batfish batfish,
      Path outputSnapshot,
      boolean printViConfigs,
      boolean printL3Topology,
      boolean printBgpTopology,
      boolean printRibs,
      boolean printBgpRibs,
      boolean printFibs,
      boolean printPrefixes) {
    try {
      System.out.println("ResultPrinter: Extracting dataplane into txt files");
      Path tmp = outputSnapshot.resolve("output").resolve("resultPrinterOutput");

      BufferedWriter bw;
      if (printViConfigs) {
        Map<String, String> configs = printConfigurations(batfish);
        for (Map.Entry<String, String> entry : configs.entrySet()) {
          bw = getBufferedWriter(tmp.resolve("json_configs").resolve(entry.getKey() + ".json"));
          bw.write(entry.getValue());
          bw.close();
        }
      }

      if (printL3Topology) {
        List<String> layer3Topology = printLayer3Topology(batfish);
        bw = getBufferedWriter(tmp.resolve("batfish_layer3_topology.txt"));
        bw.write(String.join("\n", layer3Topology));
        bw.close();
      }

      if (printBgpTopology) {
        List<String> bgpTopology = printBgpTopology(batfish);
        bw = getBufferedWriter(tmp.resolve("batfish_bgp_topology.txt"));
        bw.write(String.join("\n", bgpTopology));
        bw.close();
      }

      if (printRibs) {
        Map<String, List<String>> ribs = printRib(batfish);
        for (Map.Entry<String, List<String>> entry : ribs.entrySet()) {
          bw = getBufferedWriter(tmp.resolve("ribs").resolve(entry.getKey() + ".txt"));
          bw.write(String.join("\n", entry.getValue()));
          bw.close();
        }
      }

      if (printBgpRibs) {
        Map<String, List<String>> bgpRibs = printBgpRib(batfish);
        for (Map.Entry<String, List<String>> entry : bgpRibs.entrySet()) {
          bw = getBufferedWriter(tmp.resolve("bgpRibs").resolve(entry.getKey() + ".txt"));
          bw.write(String.join("\n", entry.getValue()));
          bw.close();
        }
      }

      if (printFibs) {
        Map<String, List<String>> fibs = printFib(batfish);
        for (Map.Entry<String, List<String>> entry : fibs.entrySet()) {
          bw = getBufferedWriter(tmp.resolve("fibs").resolve(entry.getKey() + ".txt"));
          bw.write(String.join("\n", entry.getValue()));
          bw.close();
        }
      }

      if (printPrefixes) {
        List<String> prefixes = printPrefixes(batfish);
        bw = getBufferedWriter(tmp.resolve("prefixes.txt"));
        bw.write(String.join("\n", prefixes));
        bw.close();
      }
    } catch (IOException e) {
      LOGGER.error(e);
    }
  }

  public static void printSnapshotFibs(DataPlane dataPlane, Path outputSnapshot) {
    Map<String, Map<String, Fib>> fibs = dataPlane.getFibs();
    for (Map.Entry<String, Map<String, Fib>> e1 : fibs.entrySet()) {
      String hostName = shortenHostName(e1.getKey());
      for (Map.Entry<String, Fib> e2 : e1.getValue().entrySet()) {
        try {
          BufferedWriter bw =
              getBufferedWriter(
                  outputSnapshot
                      .resolve("output")
                      .resolve("resultPrinterOutput")
                      .resolve("fibs")
                      .resolve(hostName + "-" + e2.getKey()));
          for (FibEntry fibEntry : e2.getValue().allEntries()) {
            bw.write(
                fibEntry.getTopLevelRoute().getNetwork()
                    + ",\t"
                    + fibEntry.getAction()
                    + ",\t"
                    + fibEntry.getTopLevelRoute()
                    + "\n");
          }
          bw.close();
        } catch (IOException e) {
          LOGGER.error(e);
        }
      }
    }
  }

  public static String shortenHostName(String longName) {
    if (StringUtils.isNumeric(longName)) {
      return longName.length() > 6
          ? longName.substring(0, 3) + longName.substring(longName.length() - 3)
          : longName;
    } else {
      return longName;
    }
  }

  public static Map<String, String> printConfigurations(Batfish batfish) {
    // we want to keep null values and empty lists, so we use the verbose mapper
    ObjectMapper mapper =
        BatfishObjectMapper.verboseMapper().enable(SerializationFeature.INDENT_OUTPUT);

    Map<String, String> configs = new HashMap<>();
    for (Map.Entry<String, Configuration> entry :
        batfish.loadConfigurations(batfish.getSnapshot()).entrySet()) {
      String name = shortenHostName(entry.getKey());
      Configuration c = entry.getValue();
      String config = "";
      try {
        config = mapper.writeValueAsString(c);
        //                config = BatfishObjectMapper.writePrettyString(c);
      } catch (IOException e) {
        e.printStackTrace();
      }
      configs.put(name, config);
    }
    return configs;
  }

  public static List<String> printLayer3Topology(Batfish batfish) {
    Topology layer3Topology =
        batfish.getTopologyProvider().getLayer3Topology(batfish.getSnapshot());
    Map<String, Configuration> cMap = batfish.loadConfigurations(batfish.getSnapshot());
    layer3Topology
        .getEdges()
        .forEach(
            edge -> {
              Configuration c1 = cMap.get(edge.getHead().getHostname());
              Interface if1 = c1.getAllInterfaces().get(edge.getHead().getInterface());
              Configuration c2 = cMap.get(edge.getTail().getHostname());
              Interface if2 = c2.getAllInterfaces().get(edge.getTail().getInterface());
              if (if1.getChannelGroup() != null || if2.getChannelGroup() != null) {
                System.out.println(edge);
              }
            });
    return layer3Topology.getEdges().stream()
        .map(
            edge ->
                String.join(
                    "\t",
                    shortenHostName(edge.getTail().getHostname()),
                    edge.getTail().getInterface(),
                    shortenHostName(edge.getHead().getHostname()),
                    edge.getHead().getInterface()))
        .sorted()
        .collect(Collectors.toList());
  }

  public static List<String> printBgpTopology(Batfish batfish) {
    BgpTopology bgpTopology = batfish.getTopologyProvider().getBgpTopology(batfish.getSnapshot());
    return bgpTopology.getGraph().edges().stream()
        .map(
            pair ->
                String.format(
                        "Vrf(%s, %s)",
                        shortenHostName(pair.source().getHostname()), pair.source().getVrfName())
                    + "\t"
                    + pair.target().getRemotePeerPrefix().getStartIp()
                    + "\t"
                    + String.format(
                        "Vrf(%s, %s)",
                        shortenHostName(pair.target().getHostname()), pair.target().getVrfName())
                    + "\t"
                    + pair.source().getRemotePeerPrefix().getStartIp())
        .sorted()
        .collect(Collectors.toList());
  }

  public static Map<String, List<String>> printRib(Batfish batfish) {
    Map<String, List<String>> map = new HashMap<>();
    DataPlane dataPlane = batfish.loadDataPlane(batfish.getSnapshot());
    Table<String, String, FinalMainRib> ribs = dataPlane.getRibs();
    for (Table.Cell<String, String, FinalMainRib> cell : ribs.cellSet()) {
      String hostname = shortenHostName(cell.getRowKey());
      String vrfname = cell.getColumnKey();
      List<String> list = new LinkedList<>();
      list.add(vrfname + ":\n");
      list.addAll(
          cell.getValue().getRoutes().stream()
              .map(AbstractRoute::toString)
              .collect(Collectors.toList()));
      list.add("\n");
      map.computeIfAbsent(hostname, x -> new LinkedList<>()).addAll(list);
    }
    return map;
  }

  public static Map<String, List<String>> printBgpRib(Batfish batfish) {
    Map<String, List<String>> map = new HashMap<>();
    DataPlane dataPlane = batfish.loadDataPlane(batfish.getSnapshot());
    Table<String, String, Set<Bgpv4Route>> bgpRoutes = dataPlane.getBgpRoutes();
    for (Map.Entry<String, Map<String, Set<Bgpv4Route>>> e1 : bgpRoutes.rowMap().entrySet()) {
      String hostName = shortenHostName(e1.getKey());
      List<String> list = new LinkedList<>();
      for (Map.Entry<String, Set<Bgpv4Route>> e2 : e1.getValue().entrySet()) {
        String vrfName = e2.getKey();
        list.add(vrfName);
        list.addAll(e2.getValue().stream().map(Bgpv4Route::toString).collect(Collectors.toList()));
        list.add("\n");
      }
      map.put(hostName, list);
    }
    return map;
  }

  public static Map<String, List<String>> printFib(Batfish batfish) {
    Map<String, List<String>> map = new HashMap<>();
    DataPlane dataPlane = batfish.loadDataPlane(batfish.getSnapshot());
    Map<String, Map<String, Fib>> fibs = dataPlane.getFibs();
    for (Map.Entry<String, Map<String, Fib>> e1 : fibs.entrySet()) {
      String hostName = shortenHostName(e1.getKey());
      List<String> list = new LinkedList<>();
      for (Map.Entry<String, Fib> e2 : e1.getValue().entrySet()) {
        String vrfName = e2.getKey();
        list.add(vrfName);
        list.addAll(
            e2.getValue().allEntries().stream()
                .map(
                    entry ->
                        entry.getTopLevelRoute().getNetwork()
                            + ",\t"
                            + entry.getAction()
                            + ",\t"
                            + entry.getTopLevelRoute())
                .collect(Collectors.toList()));
        list.add("\n");
      }
      map.put(hostName, list);
    }
    return map;
  }

  public static List<String> printPrefixes(Batfish batfish) {
    Set<String> prefixes = new HashSet<>();
    Map<String, Configuration> configurations = batfish.loadConfigurations(batfish.getSnapshot());

    for (Configuration c : configurations.values()) {
      for (Interface intf : c.getAllInterfaces().values()) {
        for (ConcreteInterfaceAddress addr : intf.getAllConcreteAddresses()) {
          prefixes.add(addr.getPrefix().toString());
        }
      }
      for (Vrf vrf : c.getVrfs().values()) {
        for (StaticRoute sr : vrf.getStaticRoutes()) {
          prefixes.add(sr.getNetwork().toString());
        }
        if (vrf.getBgpProcess() == null) continue;
        vrf.getBgpProcess()
            .getOriginationSpace()
            .getPrefixRanges()
            .forEach(pr -> prefixes.add(pr.getPrefix().toString()));
      }
    }

    return prefixes.stream().sorted().collect(Collectors.toList());
  }

  //  public static <R extends AbstractRouteDecorator> void writeVrfMainRibToDisk(
  //      VirtualRouterWrapper vr, Set<R> routes) {
  //    Path path =
  //        vr.getNodeWrapper()
  //            .getWorker()
  //            .getSnapshotDir()
  //            .resolve("output")
  //            .resolve("dan")
  //            .resolve("ribs")
  //            .resolve(getFullVrfName(vr));
  //    //    writeRoutesToDisk(path, routes, false);
  //  }
  //
  //  public static <R extends AbstractRouteDecorator> void writeBgpRibToDisk(
  //      VirtualRouterWrapper vr, Set<R> routes) {
  //    Path path =
  //        vr.getNodeWrapper()
  //            .getWorker()
  //            .getSnapshotDir()
  //            .resolve("output")
  //            .resolve("dan")
  //            .resolve("bgp-ribs")
  //            .resolve(getFullVrfName(vr));
  //    //    writeRoutesToDisk(path, routes, true);
  //  }

  public static <R extends AbstractRouteDecorator> void writeRoutesToDisk(
      Path path, Collection<R> routes, boolean debug) {
    if (routes.isEmpty()) {
      if (debug) {
        LOGGER.debug(
            String.format(
                "%s has no writable routes.",
                path.getName(path.getNameCount() - 2).resolve(path.getFileName())));
      }
    } else {
      String tmp = routes.stream().map(R::toString).collect(Collectors.joining("\n"));
      try {
        BufferedWriter bw = getBufferedWriter(path, true);
        bw.write(tmp);
        bw.write("\n\n");
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      tmp =
          routes.stream()
              .map(route -> route.getNetwork().toString())
              .sorted()
              .collect(Collectors.joining(", "));
      if (debug) {
        LOGGER.debug(
            String.format(
                "Write %s routes for prefixes %s to disk.",
                path.getName(path.getNameCount() - 2).resolve(path.getFileName()), tmp));
      }
    }
  }

  public static String getFullVrfName(VirtualRouter vr) {
    return vr.getConfiguration().getHostname() + "-" + vr.getName();
  }

  /** Prints results, including multiple data planes */
  public static void printSnapshotResultWithMultiDataPlane(
      Batfish batfish,
      Path outputSnapshot,
      boolean printViConfigs,
      boolean printL3Topology,
      boolean printBgpTopology,
      boolean printRibs,
      boolean printBgpRibs,
      boolean printFibs,
      boolean printPrefixes) {
    try {
      System.out.println("ResultPrinter: Extracting multi-dataplanes into txt files");
      Path tmp = outputSnapshot.resolve("output").resolve("resultPrinterOutput");

      BufferedWriter bw;
      if (printViConfigs) {
        Map<String, String> configs = printConfigurations(batfish);
        for (Map.Entry<String, String> entry : configs.entrySet()) {
          bw = getBufferedWriter(tmp.resolve("json_configs").resolve(entry.getKey() + ".json"));
          bw.write(entry.getValue());
          bw.close();
        }
      }

      if (printL3Topology) {
        List<String> layer3Topology = printLayer3Topology(batfish);
        bw = getBufferedWriter(tmp.resolve("batfish_layer3_topology.txt"));
        bw.write(String.join("\n", layer3Topology));
        bw.close();
      }

      if (printBgpTopology) {
        List<String> bgpTopology = printBgpTopology(batfish);
        bw = getBufferedWriter(tmp.resolve("batfish_bgp_topology.txt"));
        bw.write(String.join("\n", bgpTopology));
        bw.close();
      }

      if (printRibs) {
        // Get a mapping of hash -> RIBs of all data planes
        Map<String, Map<String, List<String>>> hashToRibs = printMultiRib(batfish);

        // Get a hash -> RIB mapping
        for (Map.Entry<String, Map<String, List<String>>> hashAndRib : hashToRibs.entrySet()) {
          // Get the hash of data plane
          String hash = hashAndRib.getKey();

          // RIB is split up per host. A RIB is a mapping of host -> host's RIB
          Map<String, List<String>> ribs = hashAndRib.getValue();

          for (Map.Entry<String, List<String>> entry : ribs.entrySet()) {
            bw =
                getBufferedWriter(
                    tmp.resolve("ribs")
                        .resolve(hash) // Hash of this data plane
                        .resolve(entry.getKey() + ".txt")); // Name of the host
            bw.write(String.join("\n", entry.getValue()));
            bw.close();
          }
        }
      }

      if (printBgpRibs) {
        Map<String, Map<String, List<String>>> hashToBgpRibs = printMultiBgpRib(batfish);
        for (Map.Entry<String, Map<String, List<String>>> hashAndBgpRib :
            hashToBgpRibs.entrySet()) {
          String hash = hashAndBgpRib.getKey();
          Map<String, List<String>> bgpRibs = hashAndBgpRib.getValue();
          for (Map.Entry<String, List<String>> entry : bgpRibs.entrySet()) {
            bw =
                getBufferedWriter(
                    tmp.resolve("bgpRibs").resolve(hash).resolve(entry.getKey() + ".txt"));
            bw.write(String.join("\n", entry.getValue()));
            bw.close();
          }
        }
      }

      if (printFibs) {
        Map<String, Map<String, List<String>>> hashToFibs = printMultiFib(batfish);
        for (Map.Entry<String, Map<String, List<String>>> hashAndFib : hashToFibs.entrySet()) {
          String hash = hashAndFib.getKey();
          Map<String, List<String>> fibs = hashAndFib.getValue();
          for (Map.Entry<String, List<String>> entry : fibs.entrySet()) {
            bw =
                getBufferedWriter(
                    tmp.resolve("fibs").resolve(hash).resolve(entry.getKey() + ".txt"));
            bw.write(String.join("\n", entry.getValue()));
            bw.close();
          }
        }
      }

      if (printPrefixes) {
        List<String> prefixes = printPrefixes(batfish);
        bw = getBufferedWriter(tmp.resolve("prefixes.txt"));
        bw.write(String.join("\n", prefixes));
        bw.close();
      }
    } catch (IOException e) {
      LOGGER.error(e);
    }
  }

  public static Map<String, Map<String, List<String>>> printMultiRib(Batfish batfish) {
    // Create our map of hash -> RIB of each data plane
    Map<String, Map<String, List<String>>> hashToMap = new HashMap<>();

    // Retrieve a mapping from hash to data planes
    Map<String, DataPlane> hashToDataPlane = batfish.loadMultiDataPlane(batfish.getSnapshot());

    for (Map.Entry<String, DataPlane> hashAndDP : hashToDataPlane.entrySet()) {
      // entry.getKey() is the data plane's hash (now with type String)
      // entry.getValue() is the data plane
      // For each data plane, extract its per host RIB into a Map<String, List<String>>

      DataPlane dataPlane = hashAndDP.getValue();
      Map<String, List<String>> map = new HashMap<>();
      Table<String, String, FinalMainRib> ribs = dataPlane.getRibs();
      for (Table.Cell<String, String, FinalMainRib> cell : ribs.cellSet()) {
        String hostname = shortenHostName(cell.getRowKey());
        String vrfname = cell.getColumnKey();
        List<String> list = new LinkedList<>();
        list.add(vrfname + ":\n");
        list.addAll(
            cell.getValue().getRoutes().stream()
                .map(AbstractRoute::toString)
                .collect(Collectors.toList()));
        list.add("\n");
        map.computeIfAbsent(hostname, x -> new LinkedList<>()).addAll(list);
      }

      // Put the extracted RIB into our map of hash -> RIB
      hashToMap.put(hashAndDP.getKey(), map);
    }
    return hashToMap;
  }

  public static Map<String, Map<String, List<String>>> printMultiBgpRib(Batfish batfish) {
    // Create our map of hash -> BGP RIB of each data plane
    Map<String, Map<String, List<String>>> hashToMap = new HashMap<>();

    // Retrieve a mapping from hash to data planes
    Map<String, DataPlane> hashToDataPlane = batfish.loadMultiDataPlane(batfish.getSnapshot());

    for (Map.Entry<String, DataPlane> hashAndDP : hashToDataPlane.entrySet()) {
      // entry.getKey() is the data plane's hash (now with type String)
      // entry.getValue() is the data plane
      // For each data plane, extract its BGP RIB into a Map<String, List<String>>

      DataPlane dataPlane = hashAndDP.getValue();
      Map<String, List<String>> map = new HashMap<>();
      Table<String, String, Set<Bgpv4Route>> bgpRoutes = dataPlane.getBgpRoutes();
      for (Map.Entry<String, Map<String, Set<Bgpv4Route>>> e1 : bgpRoutes.rowMap().entrySet()) {
        String hostName = shortenHostName(e1.getKey());
        List<String> list = new LinkedList<>();
        for (Map.Entry<String, Set<Bgpv4Route>> e2 : e1.getValue().entrySet()) {
          String vrfName = e2.getKey();
          list.add(vrfName);
          list.addAll(
              e2.getValue().stream().map(Bgpv4Route::toString).collect(Collectors.toList()));
          list.add("\n");
        }
        map.put(hostName, list);
      }

      // Put the extracted BGP RIB into our map of hash -> BGP RIB
      hashToMap.put(hashAndDP.getKey(), map);
    }
    return hashToMap;
  }

  public static Map<String, Map<String, List<String>>> printMultiFib(Batfish batfish) {
    // Create our map of hash -> FIB of each data plane
    Map<String, Map<String, List<String>>> hashToMap = new HashMap<>();

    // Retrieve a mapping from hash to data planes
    Map<String, DataPlane> hashToDataPlane = batfish.loadMultiDataPlane(batfish.getSnapshot());

    for (Map.Entry<String, DataPlane> hashAndDP : hashToDataPlane.entrySet()) {
      // entry.getKey() is the data plane's hash (now with type String)
      // entry.getValue() is the data plane
      // For each data plane, extract its FIB into a Map<String, List<String>>

      DataPlane dataPlane = hashAndDP.getValue();
      Map<String, List<String>> map = new HashMap<>();
      Map<String, Map<String, Fib>> fibs = dataPlane.getFibs();
      for (Map.Entry<String, Map<String, Fib>> e1 : fibs.entrySet()) {
        String hostName = shortenHostName(e1.getKey());
        List<String> list = new LinkedList<>();
        for (Map.Entry<String, Fib> e2 : e1.getValue().entrySet()) {
          String vrfName = e2.getKey();
          list.add(vrfName);
          list.addAll(
              e2.getValue().allEntries().stream()
                  .map(
                      entry ->
                          entry.getTopLevelRoute().getNetwork()
                              + ",\t"
                              + entry.getAction()
                              + ",\t"
                              + entry.getTopLevelRoute())
                  .collect(Collectors.toList()));
          list.add("\n");
        }
        map.put(hostName, list);
      }

      // Put the extracted FIB into our map of hash -> FIB
      hashToMap.put(hashAndDP.getKey(), map);
    }
    return hashToMap;
  }

  public static void outputDifference(
      Path outputSnapshot,
      boolean comparisonSubject,
      boolean compareRibs,
      boolean compareBgpRibs,
      boolean compareFibs) {
    if (comparisonSubject) {
      outputDifferenceWithPrevious(outputSnapshot, compareRibs, compareBgpRibs, compareFibs);
    } else {
      outputDifferenceWithInitial(outputSnapshot, compareRibs, compareBgpRibs, compareFibs);
    }
  }

  public static void outputDifferenceWithPrevious(
      Path outputSnapshot, boolean compareRibs, boolean compareBgpRibs, boolean compareFibs) {
    System.out.println(
        "ResultPrinter: Currently doesn't support comparing with previous dataplane.");
  }

  public static void outputDifferenceWithInitial(
      Path outputSnapshot, boolean compareRibs, boolean compareBgpRibs, boolean compareFibs) {
    System.out.println("ResultPrinter: Comparing dataplane files with initial dataplane");

    if (compareRibs) {
      try {
        Path ribsPath =
            outputSnapshot.resolve("output").resolve("resultPrinterOutput").resolve("ribs");
        FileDiffComparator comparator = new FileDiffComparator();
        Path initialPath = ribsPath.resolve("_initial");
        for (Path current : Files.newDirectoryStream(ribsPath)) {
          if (initialPath.equals(current)) {
            continue;
          }
          comparator.compareFolder(initialPath, current);
        }
      } catch (IOException e) {
        System.err.println("Error processing rib files: " + e.getMessage());
        e.printStackTrace();
      }
    }

    if (compareBgpRibs) {
      try {
        Path bgpRibsPath =
            outputSnapshot.resolve("output").resolve("resultPrinterOutput").resolve("bgpRibs");
        FileDiffComparator comparator = new FileDiffComparator();
        Path initialPath = bgpRibsPath.resolve("_initial");
        for (Path current : Files.newDirectoryStream(bgpRibsPath)) {
          if (initialPath.equals(current)) {
            continue;
          }
          comparator.compareFolder(initialPath, current);
        }
      } catch (IOException e) {
        System.err.println("Error processing bgpRib files: " + e.getMessage());
        e.printStackTrace();
      }
    }

    if (compareFibs) {
      try {
        Path fibsPath =
            outputSnapshot.resolve("output").resolve("resultPrinterOutput").resolve("fibs");
        FileDiffComparator comparator = new FileDiffComparator();
        Path initialPath = fibsPath.resolve("_initial");
        for (Path current : Files.newDirectoryStream(fibsPath)) {
          if (initialPath.equals(current)) {
            continue;
          }
          comparator.compareFolder(initialPath, current);
        }
      } catch (IOException e) {
        System.err.println("Error processing files: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }
}
