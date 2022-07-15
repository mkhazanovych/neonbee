package io.neonbee.cluster;

import static io.neonbee.test.helper.FileSystemHelper.createTempDirectory;
import static io.neonbee.test.helper.FileSystemHelper.deleteRecursiveBlocking;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import io.neonbee.Launcher;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

/**
 * Tests behavior of neon bee in cluster. Multiple instances of neon bee will be created to maintain cluster. Two nodes
 * will deploy some verticle providing data. Third vnode will deploy verticle using the data of provider verticle. Test
 * scenarios contain cases with stop and restart of nodes with provider verticles during the access to consumer verticle
 * over http in the loop to provoke error situation. Also heartbeat and failure detection features of cluster manager
 * can be then tested.
 */
@Isolated("")
class MultipleInstancesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleInstancesTest.class);

    private Map<Process, StringBuilder> processes;

    private static Path tempDirPath;

    private static String workDir;

    private final String[] args = { "-cwd", workDir, "-cl", "-cc", "hazelcast-local.xml", "-clp", "10000" };

    @BeforeAll
    static void setUp() throws IOException {
        tempDirPath = createTempDirectory();
        workDir = tempDirPath.toAbsolutePath().toString();
    }

    @AfterAll
    static void tearDown() {
        deleteRecursiveBlocking(tempDirPath);
    }

    @BeforeEach
    void beforeEach() {
        processes = new HashMap<>();
    }

    @Test
    void runInstances() throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            startProcess();
        }

        Thread.sleep(50000);
    }

    @Test
    void runLauncherInProcess() throws InterruptedException {
        Launcher.main(args);
        Thread.sleep(50000);
    }

    Process startProcess() throws IOException {
        String classPath = System.getProperty("java.class.path");
        String[] params = { "java", "-cp", classPath, "io.neonbee.Launcher" };
        var paramsList = new ArrayList<>(List.of(params));
        paramsList.addAll(List.of(args));
        var processBuilder = new ProcessBuilder(paramsList);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        LOGGER.info("started process " + process.pid());
        new Thread(() -> {
            try {
                try (BufferedReader output =
                        new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
                    var sb = new StringBuilder();
                    processes.put(process, sb);
                    while (process.isAlive()) {
                        sb.append((char) output.read());
                    }
                    sb.append("exit code " + process.exitValue());
                }
            } catch (IOException e) {
                LOGGER.error(e);
            }
        }).start();

        return process;
    }

    @AfterEach
    void afterEach() {
        if (processes != null) {
            processes.forEach((k, v) -> LOGGER.info("output from process " + k.pid() + " " + v.toString()));
            processes.keySet().forEach(Process::destroy);
        }
    }
}
