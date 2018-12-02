package com.vincentfabro.jobs;

import java.nio.ByteBuffer;
import java.util.TreeSet;

import static java.lang.System.arraycopy;

class Bloc {
    private final byte[] data;

    Bloc(byte[] data) {
        if (data.length % 32 != 0) {
            throw new IllegalArgumentException("array length must be a multiple of 32, instead is " + data.length);
        }
        this.data = data;
    }

    Bloc(TreeSet<byte[]> rawHashes) {
        var bb = ByteBuffer.allocate(32 * rawHashes.size());
        for (var hash : rawHashes) {
            if (hash.length != 32) {
                throw new IllegalArgumentException("hash must have length 32, instead is " + hash.length);
            }
            bb.put(hash);
        }
        data = bb.array();
    }

    int level() {
        var size = size();
        if (size == 0) {
            throw new IllegalArgumentException("bloc cannot be empty");
        }
        for (var level = 0; level <= 128; level++) {
            // 2^(level-1) < bloc.length <= 2^level
            if ((1 << (level - 1)) < size && size <= (1 << level)) {
                return level;
            }
        }
        throw new RuntimeException("unreachable, implementation error");
    }

    byte[] data() {
        return data;
    }

    private byte[] rawHash(int index) {
        if (data.length / 32 <= index) {
            throw new IndexOutOfBoundsException("Index out of range: " + index + ", size is " + data.length / 32);
        }
        var hash = new byte[32];
        arraycopy(data, index * 32, hash, 0, 32);
        return hash;
    }

    private int size() {
        return data.length / 32;
    }

    Bloc merged(Bloc bloc) {
        var mergedData = ByteBuffer.allocate(32 * (size() + bloc.size()));
        var b1 = 0;
        var b2 = 0;
        while (b1 < size() && b2 < bloc.size()) {
            var val1 = rawHash(b1);
            var val2 = bloc.rawHash(b2);
            if (compareHashes(val1, val2) <= 0) {
                mergedData.put(val1);
                b1++;
            }
            else {
                mergedData.put(val2);
                b2++;
            }
        }
        if (b1 < size()) {
            var offset = 32 * b1;
            mergedData.put(data, offset, data.length - offset);
        }
        if (b2 < bloc.size()) {
            var offset = 32 * b2;
            mergedData.put(bloc.data, offset, bloc.data.length - offset);
        }
        return new Bloc(mergedData.array());
    }

    static int compareHashes(byte[] hash1, byte[] hash2) {
        if (hash1.length != 32 || hash2.length != 32) {
            throw new IllegalArgumentException(
                    "hashes must have a length of 32, instead are " +
                            "(" + hash1.length + ", " + hash2.length + ")");
        }
        for (int i = 0, j = 0; i < 32 && j < 32; i++, j++) {
            int a = (hash1[i] & 0xff);
            int b = (hash2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }
}
