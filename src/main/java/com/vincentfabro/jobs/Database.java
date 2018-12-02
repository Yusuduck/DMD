package com.vincentfabro.jobs;

import io.ipfs.api.IPFS;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;

import static io.ipfs.multihash.Multihash.Type.sha2_256;
import static java.util.Arrays.copyOfRange;

class Database {
    private final static int MAX_SIZE = 100;

    private final IPFS ipfs;
    private final TreeSet<byte[]> inMemoryHashes = new TreeSet<>(Bloc::compareHashes);
    private final byte[][] blocHashes = new byte[128][];

    Database(IPFS ipfs) {
        this.ipfs = ipfs;
    }

    void add(Multihash hash) {
        inMemoryHashes.add(raw(hash));
        if (inMemoryHashes.size() == MAX_SIZE) {
            var bloc = new Bloc(inMemoryHashes);
            addToBlocs(bloc);
            inMemoryHashes.clear();
        }
    }

    private void addToBlocs(Bloc bloc) {
        var level = bloc.level();
        do {
            if (blocHashes[level] == null) {
                blocHashes[level] = save(bloc);
                break;
            }
            bloc = bloc.merged(bloc(blocHashes[level]));
            this.blocHashes[level] = null;
            level++;
        } while (true);
        saveBlocHashes();
    }

    private byte[] save(Bloc bloc) {
        try {
            return Database.raw(ipfs
                    .add(new NamedStreamable.ByteArrayWrapper(bloc.data()))
                    .get(0)
                    .hash);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void saveBlocHashes() {
        var data = ByteBuffer.allocate(64 * numberOfLevels());
        for (var level = 0; level < blocHashes.length; level++) {
            var blocHash = blocHashes[level];
            if (blocHash != null) {
                data.putInt(level);
                data.put(blocHash);
            }
        }
        try {
            var hash = ipfs.add(new NamedStreamable.ByteArrayWrapper(data.array())).get(0).hash;
            ipfs.pubsub.pub("head", hash.toBase58());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int numberOfLevels() {
        var levels = 0;
        for (var blocHash : blocHashes) {
            if (blocHash != null) {
                levels++;
            }
        }
        return levels;
    }

    private static byte[] raw(Multihash hash) {
        if (hash.type != sha2_256) {
            throw new RuntimeException("hash " + hash + " should be SHA2-256");
        }
        return copyOfRange(hash.toBytes(), 2, 34);
    }

    private Bloc bloc(byte[] rawHash) {
        try {
            return new Bloc(ipfs.cat(new Multihash(sha2_256, rawHash)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
