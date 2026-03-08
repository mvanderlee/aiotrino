import java.sql.*;
import java.util.Locale;

/**
 * JDBC Benchmark -- fetch all rows from lineitem via Trino JDBC driver.
 *
 * Usage:
 *   java -cp .:../lib/trino-jdbc.jar JdbcBenchmark [tpch|iceberg]
 *
 * Environment variables:
 *   TRINO_HOST  (default: localhost)
 *   TRINO_PORT  (default: 8085)
 */
public class JdbcBenchmark {

    static final String[][] SOURCES = {
        {"tpch",    "tpch.sf1"},
        {"iceberg", "iceberg.tpch_sf10"},
    };

    public static void main(String[] args) throws Exception {
        String source = args.length > 0 ? args[0] : "tpch";
        String schema = null;
        for (String[] s : SOURCES) {
            if (s[0].equals(source)) { schema = s[1]; break; }
        }
        if (schema == null) {
            System.err.println("Invalid source: " + source + ". Choose from: tpch, iceberg");
            System.exit(1);
        }

        String host = System.getenv().getOrDefault("TRINO_HOST", "localhost");
        String port = System.getenv().getOrDefault("TRINO_PORT", "8085");
        String url = "jdbc:trino://" + host + ":" + port;
        String query = "SELECT * FROM " + schema + ".lineitem";

        System.out.println("Source:   " + source + " (" + schema + ")");
        System.out.println("Query:    " + query);
        System.out.println("JDBC URL: " + url);
        System.out.println();

        // Benchmark
        System.out.println("--- Benchmark: JDBC ---\n");
        long t0 = System.nanoTime();
        long rows = fetchAll(url, query);
        double elapsed = (System.nanoTime() - t0) / 1_000_000_000.0;
        double throughput = rows / elapsed;

        System.out.printf(Locale.US, "%-16s %10s %12s %14s%n", "Encoding", "Rows", "Time (s)", "Rows/sec");
        System.out.println("-".repeat(56));
        System.out.printf(Locale.US, "%-16s %,10d %,12.1f %,14.0f%n", "JDBC", rows, elapsed, throughput);
        System.out.println("-".repeat(56));
        System.out.printf(Locale.US, "%nJDBC fetched %,d rows in %.1f s (%,.0f rows/sec)%n",
                rows, elapsed, throughput);
    }

    static long fetchAll(String url, String query) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, "demo", null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            int cols = rs.getMetaData().getColumnCount();
            long count = 0;
            while (rs.next()) {
                // Read every column to force full deserialization
                for (int i = 1; i <= cols; i++) {
                    rs.getObject(i);
                }
                count++;
            }
            return count;
        }
    }
}
