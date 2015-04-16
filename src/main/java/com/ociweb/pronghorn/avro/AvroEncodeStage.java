package com.ociweb.pronghorn.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorAdapter;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class AvroEncodeStage extends PronghornStage {

	private final class AvroEncoderVisitor extends StreamingReadVisitorAdapter {
					
		private final Encoder encoder;
						
		public AvroEncoderVisitor(OutputStream output) { 
					
			this.encoder = EncoderFactory.get().directBinaryEncoder(output, null);
			
		}
		
		@Override
	    public void visitTemplateOpen(String name, long id) {
            try {
                //use avro union
                
                encoder.writeLong(id); //write message id first
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }
	    
	    @Override
	    public void visitTemplateClose(String name, long id) {
	        try {
                encoder.flush();
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }
	    
	    @Override
	    public void visitFragmentOpen(String name, long id) {
	           try {
	               encoder.startItem();
	            } catch (IOException e) {
	               throw new RuntimeException(e);
	            }
	    }	    

	    @Override
	    public void visitFragmentClose(String name, long id) {
	           try {
	                encoder.flush();
	            } catch (IOException e) {
	               throw new RuntimeException(e);
	            }
	    }

	    @Override
	    public void visitSequenceOpen(String name, long id, int length) {
	        try {
	            encoder.writeArrayStart();
	            encoder.setItemCount(length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
	    }

	    @Override
	    public void visitSequenceClose(String name, long id) {
    	     try {
    	         encoder.writeArrayEnd();
             } catch (IOException e) {
                throw new RuntimeException(e);
             }
	    }

	    @Override
	    public void visitSignedInteger(String name, long id, int value) {
            try {
                encoder.writeInt(value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }

	    @Override
	    public void visitUnsignedInteger(String name, long id, long value) {
            try {
                encoder.writeInt((int)value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }

	    @Override
	    public void visitSignedLong(String name, long id, long value) {
            try {
                encoder.writeLong(value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }

	    @Override
	    public void visitUnsignedLong(String name, long id, long value) {   
            try {
                encoder.writeLong(value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	    }

	    @Override
	    public void visitDecimal(String name, long id, int exp, long mant) {   
	        
            try {
                encoder.writeInt(exp);
                encoder.writeLong(mant);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
	        
	    }

	    @Override
	    public void visitUTF8(String name, long id, Appendable value) {
            try {
                //TODO: May want a new visitor that leaves the encoded data in bytes so we can just pass it on
                encoder.writeString((CharSequence)value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            } 
	        
	    }
	    
	    @Override
	    public void visitASCII(String name, long id, Appendable value) {
	        try {
	            //TODO: May want a new visitor that leaves the encoded data in bytes so we can just pass it on
	            encoder.writeString((CharSequence)value);
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        } 
	    }

	    @Override
	    public void visitBytes(String name, long id, ByteBuffer value) {
            try {
                value.flip();
                encoder.writeFixed(value);
            } catch (IOException e) {
               throw new RuntimeException(e);
            } 
	    }

	    @Override
	    public void startup() {
	    }

	    @Override
	    public void shutdown() {
	           try {
	               encoder.flush();
	            } catch (IOException e) {
	                throw new RuntimeException(e);
	            } 
	    }


	}

	private final RingBuffer input;
	
	private final StreamingReadVisitor visitor;
	private final StreamingVisitorReader reader;
	private final OutputStream ouputStream;
	
	
	protected AvroEncodeStage(GraphManager graphManager, RingBuffer input, RingBuffer output, File averoSchemaFile) {
		super(graphManager, input, NONE);
		this.input = input;
		this.ouputStream = new RingOutputStream(output);

		assert(RingBuffer.from(output)==FieldReferenceOffsetManager.RAW_BYTES);
		valdiateMatchingSchema(input, averoSchemaFile);//new File("user.avsc")
		this.visitor = new AvroEncoderVisitor(ouputStream);
		
		this.reader = new StreamingVisitorReader(input, visitor );
		
	}

    private void valdiateMatchingSchema(RingBuffer input, File averoSchemaFile) {
        if (null==averoSchemaFile) {
            return; //skip the validation just use FROM alone
        }
        
        try {
		    FieldReferenceOffsetManager from = RingBuffer.from(input);
            Schema schema = new Schema.Parser().parse(averoSchemaFile);
            
            //TODO: AA, must validate that the schema and from agree on what the messages look like.			
            
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

	@Override
	public void startup() {
		reader.startup();
	}
	
	
	@Override
	public void run() {
		reader.run();
	}
	

	@Override
	public void shutdown() {
	    reader.shutdown();
	}


}
