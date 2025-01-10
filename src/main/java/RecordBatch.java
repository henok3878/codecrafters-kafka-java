public class RecordBatch {
   private long baseOffset; 
   private int batchLength; 
   private int partitionLeaderEpoch; 
   private byte majicByte; 
   private int CRC; 
   private short attributes; 
   private int lastOffsetDelta; 
   private long baseTimestamp; 
   private long maxTimestamp; 
   private long producerID; 
   private short producerEpoch; 
   private int baseSequence; 
   private int recordsLength; 
}
