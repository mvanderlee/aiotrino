import java.sql.*;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SimpleBenchmark {
    public static void main(String[] args) throws SQLException, IOException {
        // Parse CLI arguments
        String host = "localhost";
        String port = "8092";
        String user = "test";
        String query = null;
        String outdir = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host": host = args[++i]; break;
                case "--port": port = args[++i]; break;
                case "--user": user = args[++i]; break;
                case "--query":
                    StringBuilder sb = new StringBuilder();
                    i++;
                    while (i < args.length && !args[i].startsWith("--")) {
                        if (sb.length() > 0) sb.append(' ');
                        sb.append(args[i]);
                        i++;
                    }
                    i--; // adjust for the for-loop increment
                    query = sb.toString();
                    break;
                case "--outdir": outdir = args[++i]; break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.err.println("Usage: --host HOST --port PORT --user USER --query SQL --outdir DIR");
                    return;
            }
        }
        if (query == null || outdir == null) {
            System.err.println("Missing required args. Usage: --host HOST --port PORT --user USER --query SQL --outdir DIR");
            return;
        }
        
        String url = String.format("jdbc:trino://%s:%s/tpcds/sf100000", host, port);
        
        // Ensure output directory exists
        Path out = Paths.get(outdir).toAbsolutePath();
        Files.createDirectories(out);
        String csvFilename = out.resolve("benchmark_results_java_jdbc.csv").toString();
        File csvFile = new File(csvFilename);
        
        System.out.println("🚀 Running Java JDBC benchmark...");
        System.out.println("📝 Query: " + query);
        System.out.println();
        
        int count = 0;
        long time = 0;
        
        try (Connection conn = DriverManager.getConnection(url, user, "")) {
            long start = System.currentTimeMillis();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                while (rs.next()) {
                    count++;
                }
                
                time = System.currentTimeMillis() - start;
                double throughput = count * 1000.0 / time;
                System.out.printf("📊 %,8d rows in %,6d ms (%,8.0f rows/s)%n", count, time, throughput);
            }
        }
        
        // Append to CSV (write header if file doesn't exist)
        boolean fileExists = csvFile.exists();
        try (FileWriter writer = new FileWriter(csvFilename, true)) {
            if (!fileExists) {
                writer.write("rows,time_ms\n");
            }
            writer.write(String.format("%d,%d\n", count, time));
        }
        System.out.println();
        System.out.println("📊 Results exported to benchmark_results_java_jdbc.csv");
    }
} 