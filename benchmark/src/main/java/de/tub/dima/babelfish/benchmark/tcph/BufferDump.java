package de.tub.dima.babelfish.benchmark.tcph;

import de.tub.dima.babelfish.storage.Buffer;
import de.tub.dima.babelfish.storage.BufferManager;
import de.tub.dima.babelfish.storage.Unit;
import de.tub.dima.babelfish.storage.UnsafeUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BufferDump {

    private static long readLong(BufferedInputStream fos) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        for (int i = 0; i < 8; i++) {
            buffer.put((byte) fos.read());
        }
        return buffer.getLong(0);
    }


    private static void writeLong(BufferedOutputStream fos, long val) throws IOException {
        byte[] bytes = ByteBuffer.allocate(8).putLong(val).array();
        for (int i = 0; i < bytes.length; i++) {
            fos.write(bytes[i]);
        }
    }

    public static void dumpBuffer(String path, Buffer buffer) {
        File file = new File(path);
        try {

            try (RandomAccessFile rf = new RandomAccessFile(file, "rw")) {
                FileChannel fileChannel = rf.getChannel();
                long bufferSize = buffer.getSize().getBytes();

                long chunks = Math.floorDiv(bufferSize, Integer.MAX_VALUE) + 1;
                long currentBufferPosition = 0;
                for (long chunk = 0; chunk < chunks; chunk++) {

                    long chunkStart = chunk * (long) Integer.MAX_VALUE;
                    long chunkLength = Math.min(bufferSize - chunkStart, Integer.MAX_VALUE);

                    MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_WRITE, chunkStart, chunkLength);
                    //mb.putLong(buffer.getSize().getBytes());
                    long startAddress = buffer.getVirtualAddress().getAddress();
                    for (int i = 0; i < chunkLength; i++) {
                        mb.put(UnsafeUtils.getByte(startAddress + currentBufferPosition));
                        currentBufferPosition++;
                    }
                    System.out.println("File Cap:" + mb.capacity());  //Get the size based on content size of file
                    mb.force();
                }

                fileChannel.close();
            }

             /*   BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(file));
            long startAddress = buffer.getPhysicalAddress().getAddress();
            // write size of buffer
            writeLong(fos, buffer.getSize().getBytes());
            for (int i = 0; i < buffer.getSize().getBytes(); i++) {
                fos.write(UnsafeUtils.getByte(startAddress + i));
            }
            fos.close();*/
        } catch (FileNotFoundException e) {
            System.out.println("File not found" + e);
        } catch (IOException ioe) {
            System.out.println("Exception while writing file " + ioe);
        }
    }

    public static Buffer readBuffer(String file, BufferManager bufferManager) {

        try {
            try (RandomAccessFile rf = new RandomAccessFile(file, "r")) {
                FileChannel fileChannel = rf.getChannel();
                long bufferSize = fileChannel.size();
                Buffer buffer = bufferManager.allocateBuffer(new Unit.Bytes(bufferSize));
                long chunks = Math.floorDiv(bufferSize, Integer.MAX_VALUE) + 1;
                long currentBufferPosition = 0;
                for (long chunk = 0; chunk < chunks; chunk++) {

                    long chunkStart = chunk * (long) Integer.MAX_VALUE;
                    long chunkLength = Math.min(bufferSize - chunkStart, Integer.MAX_VALUE);
                    MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkLength);

                    // the buffer now reads the file as if it were loaded in memory.
                    System.out.println("File Cap:" + mb.capacity());  //Get the size based on content size of file

                    long startAddress = buffer.getVirtualAddress().getAddress();
                    System.out.println("start reading");
                    for (int i = 0; i < chunkLength; i++) {
                        UnsafeUtils.putByte(startAddress + currentBufferPosition, (byte) mb.get());
                        currentBufferPosition++;
                    }
                    System.out.println("Finished writing");
                }
                return buffer;
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found" + e);
        } catch (IOException ioe) {
            System.out.println("Exception while writing file " + ioe);
        }
        return null;
    }

}
