package ldbc.snb.datagen.util;

import com.google.common.collect.Streams;
import ldbc.snb.datagen.util.functional.Function;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class IteratorsTest {

    @Test
    public void testForIterator_1() {
        Function.FunctionIT mock = mock(Function.FunctionIT.class);
        Iterator iterator = Iterators.forIterator(0, i -> false, i -> ++i, mock);
        List items = (List) Streams.stream(iterator).collect(Collectors.toList());

        assertTrue(items.isEmpty());
        verifyNoInteractions(mock);
    }

    @Test
    public void testForIterator_2() {
        Function.FunctionIT mock = mock(Function.FunctionIT.class);

        when(mock.apply(0)).thenReturn(Iterators.ForIterator.CONTINUE());
        when(mock.apply(1)).thenReturn(Iterators.ForIterator.RETURN(1));
        when(mock.apply(2)).thenReturn(Iterators.ForIterator.BREAK());
        when(mock.apply(3)).thenReturn(Iterators.ForIterator.RETURN(3));

        Iterator iterator = Iterators.forIterator(0, i -> true, i -> ++i, mock);

        List items = (List) Streams.stream(iterator).collect(Collectors.toList());

        assertArrayEquals(new Integer[]{1}, items.toArray());

        verify(mock).apply(0);
        verify(mock).apply(1);
        verify(mock).apply(2);
        verify(mock, never()).apply(3);
    }
}
