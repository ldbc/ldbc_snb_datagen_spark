package ldbc.snb.datagen.util;

import ldbc.snb.datagen.util.functional.Function;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Iterators {

    public static <T> Iterator<T> concat(Iterator<? extends T> a, Iterator<? extends T> b) {
        return com.google.common.collect.Iterators.concat(a, b);
    }

    public static Iterator<Long> numbers(long start) {
        return new Iterator<Long>() {
            long i = start;
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Long next() {
                return i++;
            }
        };
    }

    public static <T> Iterator<T> emptyIterator() {
        return Collections.emptyIterator();
    }

    public static <T> Iterator<T> singletonIterator(T item) {
        return new Iterator<T>() {
            boolean hasNext = true;
            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public T next() {
                if (hasNext) {
                    hasNext = false;
                    return item;
                }
                throw new NoSuchElementException();
            }
        };
    }

    // Transforms a for loop into an iterator
    public static <T> Iterator<T> forIterator(int init, Function.FunctionIZ pre, Function.FunctionII post, Function.FunctionIT<ForIterator.Return<T>> body) {
        return new ForIterator<T>(init, pre, post, body);
    }

    // a tiny runtime for 'for loop's. Note that side effects in the body will be triggered by calling hasNext().
    public static class ForIterator<T> implements Iterator<T> {
        private int i;
        private Function.FunctionIZ pre;
        private Function.FunctionII post;
        private Function.FunctionIT<Return<T>> body;
        private Return<T> lastReturned;

        ForIterator(int init, Function.FunctionIZ pre, Function.FunctionII post, Function.FunctionIT<Return<T>> body) {
              this.i = init;
              this.pre = pre;
              this.post = post;
              this.body = body;
              this.lastReturned = null;
        }

        @Override
        public boolean hasNext() {
            if (lastReturned != null) {
                return lastReturned.tag == Return.Tag.RETURN;
            }

            lastReturned = CONTINUE();

            while (pre.apply(i) && lastReturned.tag == Return.Tag.CONTINUE) {
                lastReturned = body.apply(i);
                i = post.apply(i);
            }

            return lastReturned.tag == Return.Tag.RETURN;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            };
            T ret = lastReturned.value;
            lastReturned = null;
            return ret;
        }

        public static <T> Return<T> RETURN(T val) {
            return new Return<>(Return.Tag.RETURN, val);
        }

        public static <T> Return<T> CONTINUE() {
            return new Return<>(Return.Tag.CONTINUE, null);
        }

        public static <T> Return<T> BREAK() {
            return new Return<>(Return.Tag.BREAK, null);
        }

        public static class Return<T> {
            enum Tag {
                RETURN, // Returns a value in this iteration
                CONTINUE, // Does not return a value, but might will in the future
                BREAK // Does not return a value, and never will (iterator depleted)
            }
            Tag tag;
            T value; // null in case of 'BREAK' and 'CONTINUE'

            Return(Tag tag, T value) {
                this.tag = tag;
                this.value = value;
            }
        }
    }
}
