package io.substrait.relation;

import io.substrait.proto.WriteRel;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Write extends SingleInputRel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Write.class);

  public abstract NamedStruct getTableSchema();

  public abstract WriteRel.WriteOp getOperation();

  public abstract WriteRel.OutputMode getOutputMode();

  public abstract Optional<FileOrFiles> getFile();

  @Override
  public Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableWrite.Builder builder() {
    return ImmutableWrite.builder();
  }
}
