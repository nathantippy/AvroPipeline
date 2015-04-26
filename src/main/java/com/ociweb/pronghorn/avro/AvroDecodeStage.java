package com.ociweb.pronghorn.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class AvroDecodeStage extends PronghornStage {

    private final class AvroDecoderVisitor implements StreamingWriteVisitor {

        private final Decoder decoder;
        private final FieldReferenceOffsetManager from;
        
        public AvroDecoderVisitor(InputStream inputStream, FieldReferenceOffsetManager from) {
            
            this.decoder = DecoderFactory.get().directBinaryDecoder(inputStream, null);
            this.from = from;
            
            if (from.messageStarts.length==1) {
                //no union because there is only 1 message
                
            } else {
                //must union the messages
                
            }
            
        }

        @Override
        public boolean paused() {
            return false;
        }

        @Override
        public int pullMessageIdx() {
            try {
                //use avro union
                
                long id =  decoder.readLong();
                return FieldReferenceOffsetManager.lookupTemplateLocator(id, from);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isAbsent(String name, long id) { //This may or maynot be a useful feature.
            return false;
        }

        @Override
        public long pullSignedLong(String name, long id) {
            try {
                return decoder.readLong();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long pullUnsignedLong(String name, long id) {
            try {
                return decoder.readLong();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int pullSignedInt(String name, long id) {
            try {
                return decoder.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int pullUnsignedInt(String name, long id) {
            try {
                return decoder.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long pullDecimalMantissa(String name, long id) {
            try {
                return decoder.readLong();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int pullDecimalExponent(String name, long id) {
            try {
                return decoder.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CharSequence pullASCII(String name, long id) {
            try {
                return decoder.readString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CharSequence pullUTF8(String name, long id) {
            try {
                return decoder.readString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ByteBuffer pullByteBuffer(String name, long id) {
            try {
                return decoder.readBytes(null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int pullSequenceLength(String name, long id) {
            try{//Avro supports long for the count of elements in an array, this is not supported here yet.
                return (int)decoder.readArrayStart();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void startup() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void templateClose(String name, long id) {
        }

        @Override
        public void sequenceClose(String name, long id) {
        }

        @Override
        public void fragmentClose(String name, long id) {            
        }

        @Override
        public void fragmentOpen(String string, long l) {
            try{//Avro supports long for the count of elements in an array, this is not supported here yet.
                decoder.arrayNext();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    
    private final StreamingVisitorWriter writer;
    private final RingBuffer outputRing;
    private final InputStream inputStream;
    
    
    protected AvroDecodeStage(GraphManager graphManager, RingBuffer input, RingBuffer output, File averoSchemaFile) {
        super(graphManager, input, output);
        
        this.outputRing = output;
        this.inputStream = new RingInputStream(input);
        
        assert(RingBuffer.from(input)==FieldReferenceOffsetManager.RAW_BYTES);
        
        valdiateMatchingSchema(output, averoSchemaFile);//new File("user.avsc")
        
        AvroDecoderVisitor  visitor = new AvroDecoderVisitor(inputStream, RingBuffer.from(outputRing));
        
        writer = new StreamingVisitorWriter(outputRing, visitor );
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
        writer.startup();
    }
    
    @Override
    public void run() {
        writer.run();
    }
    
    @Override
    public void shutdown() {
        writer.shutdown();
    }

}
