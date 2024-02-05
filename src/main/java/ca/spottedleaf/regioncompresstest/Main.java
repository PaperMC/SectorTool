package ca.spottedleaf.regioncompresstest;

import ca.spottedleaf.regioncompresstest.analyse.Analyse;
import ca.spottedleaf.regioncompresstest.conversion.ConvertWorld;
import ca.spottedleaf.regioncompresstest.conversion.VerifyWorld;
import ca.spottedleaf.regioncompresstest.fuzz.FuzzProfilingTest;
import ca.spottedleaf.regioncompresstest.test.ReadProfilingTest;
import ca.spottedleaf.regioncompresstest.test.RunProfilingTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Main {

    public static final String THREADS_OPTION = "threads";
    public static final int DEFAULT_THREADS;
    static {
        final int cpus = Runtime.getRuntime().availableProcessors();
        DEFAULT_THREADS = Math.max(1, (cpus / 2) - 1);
    }

    public static final String OPERATION_PROPERTY = "op";

    public static final int THREADS = Integer.getInteger(THREADS_OPTION, DEFAULT_THREADS);
    public static final Operation OPERATION = Operation.fromProperty(System.getProperty(OPERATION_PROPERTY));

    public static void main(final String[] args) {
        if (OPERATION == null) {
            System.err.println("Bad operation, select one from: -D" + OPERATION_PROPERTY + "=[" + String.join(",", Operation.getProperties()) + "]");
            return;
        }

        if (THREADS < 0) {
            System.err.println("Threads argument: " + THREADS + " is invalid");
            return;
        }

        switch (OPERATION) {
            case TEST: {
                RunProfilingTest.run(args);
                break;
            }
            case READ: {
                ReadProfilingTest.run(args);
                break;
            }
            case CONVERT: {
                ConvertWorld.run(args);
                break;
            }
            case VERIFY: {
                VerifyWorld.run(args);
                break;
            }
            case FUZZ: {
                FuzzProfilingTest.run(args);
                break;
            }
            case ANALYSE: {
                Analyse.run(args);
                break;
            }

            default: {
                throw new IllegalStateException("Unhandled operation: " + OPERATION);
            }
        }

        return;
    }

    public static enum Operation {
        TEST("test"), READ("read"), CONVERT("conv"), VERIFY("verify"),
        FUZZ("fuzz"), ANALYSE("analyse");

        private static final Map<String, Operation> BY_PROPERTY_LC = new HashMap<>();

        static {
            for (final Operation operation : values()) {
                final Operation prev = BY_PROPERTY_LC.put(operation.property, operation);
                if (prev != null) {
                    throw new IllegalStateException("Duplicate property: " + prev + " and " + operation);
                }
            }
        }

        public final String property;

        private Operation(final String property) {
            this.property = property.toLowerCase(Locale.ROOT);
        }

        public static List<String> getProperties() {
            return new ArrayList<>(BY_PROPERTY_LC.keySet());
        }

        public static Operation fromProperty(final String property) {
            return property == null ? null : BY_PROPERTY_LC.get(property.toLowerCase(Locale.ROOT));
        }
    }
}