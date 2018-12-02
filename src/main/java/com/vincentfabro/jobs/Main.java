package com.vincentfabro.jobs;

import io.ipfs.api.IPFS;
import io.ipfs.api.NamedStreamable;
import io.ipfs.api.NamedStreamable.ByteArrayWrapper;

import java.io.IOException;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws IOException {
        var ipfs = new IPFS("/ip4/127.0.0.1/tcp/5001");
        var db = new Database(ipfs);

        for (var j = 0; j < 100; j++) {
            var infos = new ArrayList<NamedStreamable>(10);
            for (var k = 0; k < 10; k++) {
                var name = "Alice" + (j * 10 + k);
                infos.add(new ByteArrayWrapper(name.getBytes()));
            }
            System.out.println("j: " + j);
            ipfs
                    .add(infos, false, false)
                    .stream()
                    .map(node -> node.hash)
                    .forEach(hash -> {
                        System.out.println(hash);
                        db.add(hash);
                    });
        }
    }
}
