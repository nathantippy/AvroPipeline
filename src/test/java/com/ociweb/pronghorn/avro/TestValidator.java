package com.ociweb.pronghorn.avro;

import java.util.Random;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TestValidator extends PronghornStage {

    private final Random random;
    private final RingBuffer input;
    private final StreamingVisitorReader reader;
    
    public TestValidator(GraphManager gm, long seed, RingBuffer input) {
        super(gm, input, NONE);
        
        this.random = new Random(seed);
        this.input = input;
        
        StreamingReadVisitor visitor = null;
        
        this.reader = new StreamingVisitorReader(input, visitor);
        
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
