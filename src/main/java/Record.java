public class Record {
    private int length; // variable size integer indicating the length of records in this batch 
    private byte attributes; 
    private int timestampDelta; // var int indicating the timestamp difference between the timestamp of the record and the base timestamp of the record batch 
    private int offsetDelta; // var int indicating the difference between the offset of the record and the base offset of the record batch 
    private int keyLength; // var int indicating the length of the key of the record 
    private int valueLength; // var in indicating the length of the value of the record 
    // key is byte array indicating the key of record 
    private String key; // Todo: double check if string is the right one to use here 
    // Value related fields  (Feature Level Record)
    private byte frameVersion; // indicate the version of the format of the record
    private byte type; // type of the record 
    private byte version; // indicates the version of the feature level record 
    private int nameLength; // var int (unsigned) indicating the length of the name. But, since name is compact string the length of the name is always -1.
    private String name; // stored in 
    private short featureLevel; // indicating the level of the feature 
    private int taggedFieldsCount; // var int (unsigned) indicating the number of tagged fields w
    private int headersArrayCount; // unsigned var int indicating the number of headers present 
}
