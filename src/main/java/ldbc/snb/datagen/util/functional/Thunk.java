package ldbc.snb.datagen.util.functional;

@FunctionalInterface
public interface Thunk {
    void apply() throws Exception;

    static void wrapException(Thunk t) {
        try {
            t.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
