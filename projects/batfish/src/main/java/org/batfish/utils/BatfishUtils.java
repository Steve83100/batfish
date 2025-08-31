package org.batfish.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.batfish.common.BatfishLogger;
import org.batfish.common.BfConsts;
import org.batfish.common.NetworkSnapshot;
import org.batfish.common.util.CommonUtil;
import org.batfish.config.Settings;
import org.batfish.datamodel.Configuration;
import org.batfish.datamodel.DataPlane;
import org.batfish.datamodel.collections.BgpAdvertisementsByVrf;
//import org.batfish.dataplane.ibdp.CentralizedDataPlanePlugin;
//import org.batfish.dataplane.ibdp.DistributedDataPlanePlugin;
import org.batfish.dataplane.ibdp.IncrementalDataPlanePlugin;
import org.batfish.main.Batfish;
import org.batfish.storage.FileBasedStorage;
import org.batfish.vendor.VendorConfiguration;

public class BatfishUtils {
  public static final Path NETWORKS_BASE = Paths.get("/home/steven/BatfishModification/exampleNetworks");

  public static final String CONTAINERS = "containers";
  public static final String NETWORKS = "networks";
  public static final String SNAPSHOTS = "snapshots";
  public static final String INPUT = "input";
  public static final String OUTPUT = "output";
  public static final String EXTERNAL_BGP_ADV = "external_bgp_announcements.json";

  public static class Param {
    final Path storageBase;
    final String networkName;
    final String snapshotName;
    final Map<String, String> configurationTexts;
    @Nullable final Path layer1Topology;
    final boolean useCache;
//    final boolean distributed;

    public Param(
        Path storageBase,
        String networkName,
        String snapshotName,
        Map<String, String> configurationTexts,
        @Nullable Path layer1Topology,
        boolean useCache
//        ,boolean distributed
    ) {
      this.storageBase = storageBase;
      this.networkName = networkName;
      this.snapshotName = snapshotName;
      this.configurationTexts = configurationTexts;
      this.layer1Topology = layer1Topology;
      this.useCache = useCache;
//      this.distributed = distributed;
    }
  }

  public static class ParamBuilder {
    Path storageBase = NETWORKS_BASE;
    String networkName = UUID.randomUUID().toString();
    String snapshotName = timestamp();
    String snapshotNameSuffix;
    Map<String, String> configurationTexts;
    Path layer1Topology;
    boolean useCache = false;
//    boolean distributed = true;

    public ParamBuilder() {}

    public Param build() {
      return new Param(
          storageBase,
          networkName,
          snapshotName + (snapshotNameSuffix == null ? "" : "-" + snapshotNameSuffix),
          configurationTexts,
          layer1Topology,
          useCache
//          ,distributed
      );
    }

    public ParamBuilder setStorageBase(Path storageBase) {
      this.storageBase = storageBase;
      return this;
    }

    public ParamBuilder setNetworkName(String networkName) {
      this.networkName = networkName;
      return this;
    }

    public ParamBuilder setSnapshotName(String snapshotName) {
      this.snapshotName = snapshotName;
      return this;
    }

    public ParamBuilder setSnapshotNameSuffix(String snapshotNameSuffix) {
      this.snapshotNameSuffix = snapshotNameSuffix;
      return this;
    }

    public ParamBuilder setConfigurationPaths(List<String> configurationPaths) {
      try {
        this.configurationTexts = getTextFromConfigs(configurationPaths);
      } catch (IOException e) {
        System.out.println("Could not read Configuration Files!");
        e.printStackTrace();
      }
      return this;
    }

    public ParamBuilder setConfigurationTexts(Map<String, String> configurationTexts) {
      this.configurationTexts = configurationTexts;
      return this;
    }

    public ParamBuilder setLayer1Topology(@Nullable Path layer1Topology) {
      this.layer1Topology = layer1Topology;
      return this;
    }

    public ParamBuilder setUseCache(boolean useCache) {
      this.useCache = useCache;
      return this;
    }

//    public ParamBuilder setDistributed(boolean distributed) {
//      this.distributed = distributed;
//      return this;
//    }
  }

  public static String timestamp() {
    Date date = new Date();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    return simpleDateFormat.format(date);
  }

  public static Pair<Path, Batfish> getBatfishFromTestrigText(
      Path storageBase,
      String networkName,
      @Nullable String snapshotSuffix,
      Map<String, String> configurationTexts,
      @Nullable Path layer1Topology) {
    String snapshotName = timestamp();
    return getBatfishFromTestrigText(
        storageBase,
        networkName,
        snapshotName + (snapshotSuffix == null ? "" : "-" + snapshotSuffix),
        configurationTexts,
        layer1Topology,
        false
//        ,true
    );
  }

  /**
   * @param storageBase path to the top-level output folder
   * @param networkName network name
   * @param snapshotName snapshot name, default current time
   * @param configurationTexts map from hostnames to configuration files
   * @param layer1Topology path to the layer1 topology
   * @param useCache whether use cached vendor independent {@link Configuration}s or not.
   *
   * @param distributed whether use distributed data plane plugin {@link
   *     org.batfish.dataplane.ibdp.DistributedDataPlanePlugin} or not.
   *
   * @return a pair of (1) snapshot output folder, and (2) the batfish object
   */
  public static Pair<Path, Batfish> getBatfishFromTestrigText(
      Path storageBase,
      String networkName,
      String snapshotName,
      Map<String, String> configurationTexts,
      @Nullable Path layer1Topology,
      boolean useCache
//      ,boolean distributed
  ) {
    return getBatfishFromTestrigText(
        new ParamBuilder()
            .setStorageBase(storageBase)
            .setNetworkName(networkName)
            .setSnapshotName(snapshotName)
            .setConfigurationTexts(configurationTexts)
            .setLayer1Topology(layer1Topology)
            .setUseCache(useCache)
//            .setDistributed(distributed)
            .build());
  }

  public static Pair<Path, Batfish> getBatfishFromTestrigText(Param param) {
    initContainer(param.storageBase, param.networkName, param.snapshotName, param.useCache);

    Settings settings = settings(param.storageBase, param.networkName, param.snapshotName);

    Path snapshotDir = getSnapshotPath(param.storageBase, param.networkName, param.snapshotName);
    writeTemporaryTestrigFiles(
        param.configurationTexts,
        snapshotDir.resolve(INPUT).resolve(BfConsts.RELPATH_CONFIGURATIONS_DIR));
    if (param.layer1Topology != null && param.layer1Topology.toFile().exists()) {
      try {
        Files.copy(
            param.layer1Topology,
            snapshotDir.resolve(INPUT).resolve(BfConsts.RELPATH_L1_TOPOLOGY_PATH));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    Batfish batfish =
        new Batfish(
            settings,
            makeTestrigCache(),
            makeDataPlaneCache(),
            makeEnvBgpCache(),
            makeVendorConfigurationCache(),
            new FileBasedStorage(settings.getStorageBase(), settings.getLogger()),
            null);

    registerDataPlanePlugins(batfish
//            , param.distributed
    );

    return Pair.of(snapshotDir, batfish);
  }

  public static Pair<Path, Batfish> getBatfishFromConfiguration(
      Path storageBase,
      String networkName,
      @Nullable String snapshotSuffix,
      SortedMap<String, Configuration> configurations,
      @Nullable Path layer1Topology,
      boolean useCache
//      ,boolean distributed
  ) {
    String snapshotName = timestamp() + (snapshotSuffix == null ? "" : "-" + snapshotSuffix);

    initContainer(storageBase, networkName, snapshotName, useCache);

    Settings settings = settings(storageBase, networkName, snapshotName);

    Path snapshotDir = getSnapshotPath(storageBase, networkName, snapshotName);
    if (layer1Topology != null && layer1Topology.toFile().exists()) {
      try {
        Files.copy(
            layer1Topology, snapshotDir.resolve(INPUT).resolve(BfConsts.RELPATH_L1_TOPOLOGY_PATH));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    Cache<NetworkSnapshot, SortedMap<String, Configuration>> testrigCache = makeTestrigCache();
    NetworkSnapshot snapshot =
        new NetworkSnapshot(
            Objects.requireNonNull(settings.getContainer()),
            Objects.requireNonNull(settings.getTestrig()));
    testrigCache.put(snapshot, configurations);

    Batfish batfish =
        new Batfish(
            settings,
            testrigCache,
            makeDataPlaneCache(),
            makeEnvBgpCache(),
            makeVendorConfigurationCache(),
            new FileBasedStorage(settings.getStorageBase(), settings.getLogger()),
            null);

    registerDataPlanePlugins(batfish
//            , distributed
    );

    return Pair.of(snapshotDir, batfish);
  }

  public static Settings settings(Path storageBase, String networkName, String snapshotName) {
    Settings settings = new Settings(new String[] {});

    settings.setLogger(new BatfishLogger("debug", false));
    settings.setDisableUnrecognized(true);
    settings.setHaltOnConvertError(true);
    settings.setHaltOnParseError(true);
    settings.setThrowOnLexerError(true);
    settings.setThrowOnParserError(true);
    settings.setVerboseParse(true);

    settings.setStorageBase(storageBase.resolve(CONTAINERS));

    settings.setContainer(networkName); // networkId
    settings.setTestrig(snapshotName); // snapshotId
    settings.setSnapshotName(snapshotName); // snapshotName

    return settings;
  }

  public static Path getNetworkPath(Path storageBase, String networkName) {
    return Paths.get(storageBase.toString(), CONTAINERS, NETWORKS, networkName);
  }

  public static Path getSnapshotPath(Path storageBase, String networkName, String snapshotName) {
    return getNetworkPath(storageBase, networkName).resolve(SNAPSHOTS).resolve(snapshotName);
  }

  private static void initContainer(
      Path storageBase, String networkName, String snapshotName, boolean useCache) {
    Path snapshotDir = getSnapshotPath(storageBase, networkName, snapshotName);
    Path networkBlobs = snapshotDir.getParent().getParent().resolve("blobs");
    try {
      if (!useCache) FileUtils.deleteDirectory(networkBlobs.toFile());
      if (snapshotDir.toFile().exists()) FileUtils.deleteDirectory(snapshotDir.toFile());
      Files.createDirectories(snapshotDir.resolve(INPUT));
      Files.createDirectories(snapshotDir.resolve(OUTPUT));
    } catch (IOException ignored) {
      //      e.printStackTrace();
    }
  }

  private static void registerDataPlanePlugins(Batfish batfish
//          , boolean distributed
  ) {
    IncrementalDataPlanePlugin ibdpPlugin = new IncrementalDataPlanePlugin();
    ibdpPlugin.initialize(batfish);
  }

  public static Map<String, String> getTextFromConfigs(List<String> configurationNames)
      throws IOException {
    SortedMap<String, String> configurationTextMap = new TreeMap<>();
    for (String configName : configurationNames) {
      byte[] encoded = Files.readAllBytes(Paths.get(configName));
      String configurationText = new String(encoded, Charset.defaultCharset());
      configurationTextMap.put(new File(configName).getName(), configurationText);
    }
    return configurationTextMap;
  }

  private static void writeTemporaryTestrigFiles(
      Map<String, String> filesText, Path outputDirectory) {
    if (filesText != null) {
      boolean b = outputDirectory.toFile().mkdirs();
      if (!b) System.err.println("mkDirs() failed for" + outputDirectory);
      filesText.forEach(
          (filename, text) -> CommonUtil.writeFile(outputDirectory.resolve(filename), text));
    }
  }

  private static Cache<NetworkSnapshot, SortedMap<String, Configuration>> makeTestrigCache() {
    return CacheBuilder.newBuilder().softValues().maximumSize(5).build();
  }

  private static Cache<NetworkSnapshot, DataPlane> makeDataPlaneCache() {
    return CacheBuilder.newBuilder().softValues().maximumSize(2).build();
  }

  private static Map<NetworkSnapshot, SortedMap<String, BgpAdvertisementsByVrf>> makeEnvBgpCache() {
    return Collections.synchronizedMap(new LRUMap<>(4));
  }

  private static Cache<NetworkSnapshot, Map<String, VendorConfiguration>>
      makeVendorConfigurationCache() {
    return CacheBuilder.newBuilder().softValues().maximumSize(2).build();
  }
}
