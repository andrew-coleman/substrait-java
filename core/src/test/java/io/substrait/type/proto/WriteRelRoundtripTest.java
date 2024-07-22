package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.proto.WriteRel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.Write;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.relation.files.ImmutableFileFormat;
import io.substrait.relation.files.ImmutableFileOrFiles;
import io.substrait.type.NamedStruct;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class WriteRelRoundtripTest extends TestBase {

  @Test
  void insert() {
    NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    VirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                ExpressionCreator.struct(
                    false, ExpressionCreator.i64(false, 1), ExpressionCreator.i64(false, 2)))
            .build();
    virtTable =
        VirtualTableScan.builder()
            .from(virtTable)
            .filter(b.equal(b.fieldReference(virtTable, 0), b.fieldReference(virtTable, 1)))
            .build();

    Write command =
        Write.builder()
            .input(virtTable)
            .tableSchema(schema)
            .operation(WriteRel.WriteOp.WRITE_OP_INSERT)
            .outputMode(WriteRel.OutputMode.OUTPUT_MODE_NO_OUTPUT)
            .file(
                ImmutableFileOrFiles.builder()
                    .fileFormat(ImmutableFileFormat.ParquetReadOptions.builder().build())
                    .partitionIndex(0)
                    .start(0)
                    .length(100)
                    .path("test/path")
                    .pathType(FileOrFiles.PathType.URI_FILE)
                    .build())
            .build();

    verifyRoundTrip(command);
  }
}
