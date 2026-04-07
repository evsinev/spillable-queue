package com.payneteasy.spillable_queue.impl;

import com.payneteasy.spillable_queue.ISpillableQueueSerializer;

import java.io.*;

/**
 * Default serializer using standard Java {@link ObjectOutputStream} /
 * {@link ObjectInputStream}. Works with any {@link Serializable} type
 * out of the box, but is not the fastest option for high-throughput
 * scenarios — consider Kryo or Protobuf for production loads.
 */
public class SpillableQueueSerializerJavaSerImpl<E extends Serializable> implements ISpillableQueueSerializer<E> {

    @Override
    public byte[] serialize(E element) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(element);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Serialization failed", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public E deserialize(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (E) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Deserialization failed", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found during deserialization", e);
        }
    }
}
