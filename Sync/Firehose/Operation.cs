using PeterO.Cbor;

namespace Sync.Firehose;

public class Operation
{
    public required byte[] Cid { get; set; }
    public required string Action { get; set; }
    public required string Path { get; set; }
    
    public CBORObject CidRaw { get; set; }
    
    public static Operation FromCborObject(CBORObject cbor)
    {
        return new Operation
        {
            Cid = cbor["cid"].GetByteString(),
            CidRaw = cbor["cid"],
            Action = cbor["action"].AsString(),
            Path = cbor["path"].AsString()
        };
    }
}