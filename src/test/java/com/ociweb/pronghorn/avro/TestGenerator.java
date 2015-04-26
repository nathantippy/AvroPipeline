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
     *                Object event interfaces? for existing API methods that reqire objects
     *                write/read fields in order -> low level or visitor
     *                write/read out of order -> high level
     *                Other: Code generated POJOs - Objects that need to leave scope, 
     *                Other: Code generated Flyweight - beter performance than proxy.
     *                
     *                
     *                                 Performant    RuntimeFields   Nested  Objects  DataRoute  OutOfOrder  ManyMessages
     *                LowLevel             X              X                              X                    
     *                HighLevel            X              X/           X                 X          X             X
     *                Visitor              X/             X            X                                          X
     *                Proxy                                                     X                   X
     *                
     *                DTO - POJO                                       ?        X                   X             X
     *                GenFly               X                           ?        X                   X             
     *                
     *                
     *                
     *                LowLevel  ->  Visitor
     *                HighLievel -> Proxy
     *                           -> DTO
     *                           -> GenFly
     *                
     *                
     *                
     *                
     *                
     *                
     *  
     * 
     */
    
    private final StreamingVisitorWriter writer;
    private final RingBuffer output;
    
    public TestGenerator(GraphManager gm, long seed, RingBuffer output) {
        super(gm, NONE, output);
        
        this.output = output;
        StreamingWriteVisitor visitor = new GeneratorWriterVisitor(RingBuffer.from(output), new Random(seed), 30, 30);        
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
