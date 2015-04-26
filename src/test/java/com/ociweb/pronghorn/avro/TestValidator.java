package com.ociweb.pronghorn.avro;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TestValidator extends PronghornStage {
    
    private final RingBuffer expectedInput;
    private final RingBuffer checkedInput;
    
    private final StreamingVisitorReader reader;
    
    public TestValidator(GraphManager gm, RingBuffer ... input) {
        super(gm, input, NONE);
        
        this.expectedInput = input[0];
        this.checkedInput = input[1];
        
        
        StreamingReadVisitor visitor = new MatchingReadVisitor(expectedInput);
        
        this.reader = new StreamingVisitorReader(checkedInput, visitor);
        
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
