package io.github.yok.flexdblink.junit;

import java.util.Objects;
import org.mockito.Mockito;
import org.springframework.lang.NonNull;

/**
 * Creates mocks with an explicit non-null return contract for Spring API calls.
 */
final class TestMocks {

    /** Prevents instantiation of the mock factory. */
    private TestMocks() {}

    /**
     * Creates a non-null mock of the requested type.
     *
     * @param type class or interface to mock
     * @param <T> mocked type
     * @return non-null Mockito mock
     */
    @NonNull
    static <T> T mockNonNull(Class<T> type) {
        return Objects.requireNonNull(Mockito.mock(type));
    }
}
