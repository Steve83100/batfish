package org.batfish.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.batfish.common.BatfishException;

public class StorageUtils {
  public static void cleanDirectory(Path dir) {
    try {
      if (dir.toFile().exists()) {
        FileUtils.cleanDirectory(dir.toFile());
      }
    } catch (IOException e) {
      throw new BatfishException("Failed to clean directory: '" + dir, e);
    }
  }

  public static void copyDirectory(Path srcPath, Path dstPath) {
    try {
      FileUtils.copyDirectory(srcPath.toFile(), dstPath.toFile());
    } catch (IOException e) {
      throw new BatfishException(
          "Failed to copy directory: '" + srcPath + "' to: '" + dstPath + "'", e);
    }
  }

  public static Path createIfNotExist(Path path) {
    File file = path.toFile();
    if (!file.exists()) {
      if (file.isDirectory()) {
        file.mkdirs();
      } else {
        file.getParentFile().mkdirs();
      }
    }
    return path;
  }

  public static BufferedWriter getBufferedWriter(Path path) throws IOException {
    return getBufferedWriter(path, false);
  }

  public static BufferedWriter getBufferedWriter(Path path, boolean append) throws IOException {
    File file = createIfNotExist(path).toFile();
    return new BufferedWriter(new FileWriter(file, append));
  }

  public static void exportToMetisFormat(
      List<Set<Integer>> adjacencyMatrix, Path inputFile, Map<Integer, Integer> id2Weight) {

    int numNodes = adjacencyMatrix.size() - 1;
    int numEdges = 0;
    for (Set<Integer> row : adjacencyMatrix) {
      numEdges += row.size();
    }
    numEdges /= 2;

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile.toFile()))) {
      writer.write(numNodes + " " + numEdges + " 011");
      writer.newLine();

      for (int i = 1; i <= numNodes; i++) {
        Set<Integer> neighbors = adjacencyMatrix.get(i);
        StringBuilder line = new StringBuilder();

        line.append(id2Weight.get(i)).append(" ");

        for (int neighbor : neighbors) {
          line.append(neighbor)
              .append(" ")
              .append(id2Weight.get(neighbor) + id2Weight.get(i))
              .append(" ");
        }

        writer.write(line.toString());
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
