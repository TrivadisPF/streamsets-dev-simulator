package com.trivadis.streamsets.sdc.util;

public class ExceptionUtils {

    private ExceptionUtils() {}

    /**
     * Throws an undeclared checked exception, use with caution.
     */
    public static void throwUndeclared(Throwable ex) {
        ExceptionUtils.<RuntimeException>_throw(ex);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Exception> void _throw(Throwable e) throws E {
        throw (E)e;
    }

    @SuppressWarnings("unchecked")
    public static <E extends Throwable> E findSpecificCause(Exception ex, Class<E> causeClass) {
        Throwable cause = ex.getCause();
        while (cause != null && ! causeClass.isInstance(cause)) {
            cause = cause.getCause();
        }
        return (E) cause;
    }

}