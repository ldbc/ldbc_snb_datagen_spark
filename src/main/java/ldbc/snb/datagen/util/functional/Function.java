package ldbc.snb.datagen.util.functional;

public class Function {

    // Specialized interfaces to prevent boxing

    @FunctionalInterface
    public interface FunctionIZ {
        boolean apply(int value);
    }

    @FunctionalInterface
    public interface FunctionII {
        int apply(int value);
    }

    @FunctionalInterface
    public interface FunctionIT<T> {
        T apply(int value);
    }
}
