package com.ociweb.pronghorn.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorAdapter;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class OutputAveroStage extends PronghornStage {

	private final class ExampleVisitor extends StreamingReadVisitorAdapter {
		
		private final DataFileWriter<GenericRecord> dataFileWriter;
		private final Schema schema;				
		private GenericRecord activeRecord;
		
		public ExampleVisitor(DataFileWriter<GenericRecord> dataFileWriter, Schema schema, OutputStream output, FieldReferenceOffsetManager from) { 
		
			
		    this.dataFileWriter = dataFileWriter;
			this.schema = schema;
			
		}


		@Override
		public void visitTemplateOpen(String name, long id) {
			activeRecord = new GenericData.Record(schema);
		}
		
		
		
		@Override
		public void visitBytes(String name, long id, ByteBuffer value) {			
			value.flip();
			byte[] target = new byte[value.remaining()];
			value.get(target);			
			activeRecord.put(name, target);//TODO: B, we do an extra copy here that would be nice to avoid.
		}

		@Override
		public void visitSignedInteger(String name, long id, int value) {
			activeRecord.put(name, new Integer(value));
		}

		@Override
		public void visitUnsignedInteger(String name, long id, long value) {
			activeRecord.put(name, new Long(value));
		}
		
		@Override
		public void visitASCII(String name, long id, Appendable value) {
		   
			activeRecord.put(name, value.toString());//TODO: B, we do an extra copy here that would be nice to avoid.
			
		}


		@Override
		public void visitTemplateClose(String name, long id) {
			try {
				dataFileWriter.append(activeRecord); //TODO: AA, chagnge to append to pre-encoded to avoid above object creation
				activeRecord = null;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}


		@Override
		public void startup() {
			try {
				dataFileWriter.create(schema, output);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}


		@Override
		public void shutdown() {			
				try {
					dataFileWriter.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}			
		}

	}

	private final RingBuffer input;
	
	private StreamingReadVisitor visitor;
	private StreamingVisitorReader reader;
	private FieldReferenceOffsetManager from;
	private File averoSchemaFile;
	private OutputStream output;
	
	
	protected OutputAveroStage(GraphManager graphManager, RingBuffer input, OutputStream output, File averoSchemaFile) {
		super(graphManager, input, NONE);
		this.input = input;
		this.averoSchemaFile = averoSchemaFile; //new File("user.avsc")
		this.output = output;
	}

	@Override
	public void startup() {
		super.startup();		
		
		try{
			
			from = RingBuffer.from(input);
			Schema schema = new Schema.Parser().parse(averoSchemaFile);

			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
						
			visitor = new ExampleVisitor(dataFileWriter, schema, output,  from);
						
			reader = new StreamingVisitorReader(input, visitor );
			
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER TARGET FOR INFORMATION
			//////
			
			reader.startup();
								
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
		reader.run();
	}
	

	@Override
	public void shutdown() {
		
		try{
			reader.shutdown();
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER TARGET OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}


}
