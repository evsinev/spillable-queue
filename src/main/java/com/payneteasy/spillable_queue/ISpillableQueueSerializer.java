package com.payneteasy.spillable_queue;

import com.payneteasy.spillable_queue.impl.SpillableQueueSerializerJavaSerImpl;

import java.io.Serializable;

/**
 * Pluggable serialization strategy for queue elements spilled to disk.
 * <p>
 * The default implementation ({@link SpillableQueueSerializerJavaSerImpl}) uses standard Java
 * serialization, but you can provide a faster alternative (Kryo, Protobuf,
 * Jackson, etc.) via this interface.
 *
 * @param <E> element type
 */
public interface ISpillableQueueSerializer<E extends Serializable> {

    byte[] serialize(E element);

    E deserialize(byte[] data);
}
