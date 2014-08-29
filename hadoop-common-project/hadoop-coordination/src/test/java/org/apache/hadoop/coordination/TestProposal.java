package org.apache.hadoop.coordination;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public abstract class TestProposal {

  public byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(obj);
    oos.close();
    return bytes.toByteArray();
  }

  public Object deserialize(byte[] data)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytes = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bytes);
    Object obj = ois.readObject();
    ois.close();
    return obj;
  }
}
