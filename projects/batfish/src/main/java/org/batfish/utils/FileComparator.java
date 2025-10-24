package org.batfish.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
// import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FileComparator {

  private Set<String> getFileNames(Path folder) throws IOException {
    return Files.list(folder)
        .filter(Files::isRegularFile)
        .map(path -> path.getFileName().toString())
        .collect(Collectors.toSet());
  }

  public void compareFolder(Path referenceFolder, Path currentFolder) {
    try {
      Set<String> referenceFiles = getFileNames(referenceFolder);
      Set<String> currentFiles = getFileNames(currentFolder);

      // Find common files
      Set<String> commonFiles = new HashSet<>(referenceFiles);
      commonFiles.retainAll(currentFiles);

      if (commonFiles.isEmpty()) {
        System.out.println(
            "  No common files found between "
                + referenceFolder.getFileName()
                + " and "
                + currentFolder.getFileName());
        return;
      }

      // Create consolidated diff file
      String consolidatedDiffFileName = currentFolder.getFileName() + "_differences.txt";
      Path consolidatedDiffFile = currentFolder.resolve(consolidatedDiffFileName);

      try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(consolidatedDiffFile))) {
        // Write header
        writer.println("# Difference for folder: " + currentFolder.getFileName());
        writer.println("# Compared with folder: " + referenceFolder.getFileName());
        writer.println();

        int identicalCount = 0;
        int differentCount = 0;

        // Sort files for consistent output
        List<String> sortedCommonFiles = commonFiles.stream().sorted().collect(Collectors.toList());

        // Compare each common file and write to consolidated file
        for (String fileName : sortedCommonFiles) {
          Path referenceFile = referenceFolder.resolve(fileName);
          Path currentFile = currentFolder.resolve(fileName);

          // Write section header for this file
          writer.println("## " + "=".repeat(78));
          writer.println("## File: " + fileName);

          // Get diff result and write it
          DiffResult diffResult = compareSingleFile(referenceFile, currentFile);
          //          writer.println("Reference: " + referenceFile);
          //          writer.println("Current:   " + currentFile);
          //          writer.println();

          if (diffResult.isIdentical()) {
            writer.println("Files are identical - no differences found.");
            writer.println();
            identicalCount++;
//            System.out.println("  " + fileName + " - No differences (identical files)");
          } else if (diffResult.hasDifferences()) {
            writer.println("Differences found:");
            writer.println(diffResult.getDiffOutput());
            differentCount++;
//            System.out.println("  " + fileName + " - Differences found");
          } else {
            writer.println("Error running diff command:");
            writer.println(diffResult.getErrorMessage());
            writer.println();
            System.err.println("  " + fileName + " - Error during comparison");
          }
        }
        writer.println();
        writer.println("# " + "=".repeat(80));
        writer.println(
            "Summary: " + identicalCount + " identical, " + differentCount + " different files");
      }

    } catch (IOException e) {
      System.err.println(
          "  Error processing folder " + currentFolder.getFileName() + ": " + e.getMessage());
    }
  }

  private DiffResult compareSingleFile(Path referenceFile, Path currentFile) {
    try {
      // Build diff command
      ProcessBuilder pb =
          new ProcessBuilder("diff", "-U 1", referenceFile.toString(), currentFile.toString());

      Process process = pb.start();

      // Capture output
      StringBuilder output = new StringBuilder();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append("\n");
        }
      }

      // Capture error output
      StringBuilder errorOutput = new StringBuilder();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          errorOutput.append(line).append("\n");
        }
      }

      int exitCode = process.waitFor();

      if (exitCode == 0) {
        return new DiffResult(true, false, "", "");
      } else if (exitCode == 1) {
        return new DiffResult(false, true, output.toString(), "");
      } else {
        return new DiffResult(
            false,
            false,
            "",
            "Diff command failed with exit code "
                + exitCode
                + (errorOutput.length() > 0 ? "\nError: " + errorOutput.toString() : ""));
      }

    } catch (IOException | InterruptedException e) {
      return new DiffResult(false, false, "", "Exception occurred: " + e.getMessage());
    }
  }

  // Helper class to encapsulate diff results
  private static class DiffResult {
    private final boolean identical;
    private final boolean hasDifferences;
    private final String diffOutput;
    private final String errorMessage;

    public DiffResult(
        boolean identical, boolean hasDifferences, String diffOutput, String errorMessage) {
      this.identical = identical;
      this.hasDifferences = hasDifferences;
      this.diffOutput = diffOutput;
      this.errorMessage = errorMessage;
    }

    public boolean isIdentical() {
      return identical;
    }

    public boolean hasDifferences() {
      return hasDifferences;
    }

    public String getDiffOutput() {
      return diffOutput;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }
}