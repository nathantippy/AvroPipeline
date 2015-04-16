package com.ociweb.pronghorn.avro;

import java.util.Random;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TestGenerator extends PronghornStage {

    /*
     * API Decision: (TODO: build table)
     *                Do not know the fields at compile time -> visitors or low level
     *                Only 1 message type -> Low level easier
     *                Specific business logic on fields -> High level perhaps visitor
     *                Complex nested structure -> not low level.
     *                Bulk data routing -> low level, perhaps high level.
     *                Object event interfaces?
     *                write/read fields in order -> low level or visitor
     *                write/read out of order -> high level
     * 
     * 
     */
    
    
    private final Random random;
    private final StreamingVisitorWriter writer;
    private final RingBuffer output;
    
    public TestGenerator(GraphManager gm, long seed, RingBuffer output) {
        super(gm, NONE, output);
       
        this.random = new Random(seed);
        this.output = output;
        StreamingWriteVisitor visitor = null;        
        this.writer = new StreamingVisitorWriter(output, visitor  );
        
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
