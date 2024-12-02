using System.Formats.Cbor;
using System.Net.WebSockets;
using System.Text;
using IPLD.ContentIdentifier;
using Multiformats.Base;
using PeterO.Cbor;
using Multiformats.Codec;
using Multiformats.Hash;

namespace Sync.Firehose;

/**
 * Based on the official Typescript implementation,
 * hope to emulate that once we have a working version.
 */
public class FirehoseOptions
{
    public string Service { get; set; } = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
    public Func<FirehoseEvent, Task>? HandleEvent { get; set; }
    public Action<Exception>? OnError { get; set; }
    public bool UnauthenticatedCommits { get; set; } = false;
    public bool UnauthenticatedHandles { get; set; } = false;
    public List<string>? FilterCollections { get; set; } = [];
    public bool ExcludeIdentity { get; set; } = false;
    public bool ExcludeAccount { get; set; } = false;
    public bool ExcludeCommit { get; set; } = false;
}
public record FirehoseEvent
{
    public required string Event { get; set; }
    public required object Data { get; set; }
}


public class FirehoseClient(FirehoseOptions options)
{
    private ClientWebSocket _webSocket = null!;
    static byte[] HexStringToByteArray(string hex)
    {
        int numberChars = hex.Length;
        byte[] bytes = new byte[numberChars / 2];
        for (int i = 0; i < numberChars; i += 2)
        {
            bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
        }
        return bytes;
    }
    public async Task StartAsync()
    {
        _webSocket = new ClientWebSocket();

        // Log outgoing request headers
        Console.WriteLine("Connecting to WebSocket server...");
        Console.WriteLine($"Service URL: {options.Service}");

        try
        {
            await _webSocket.ConnectAsync(new Uri(options.Service), CancellationToken.None);
            Console.WriteLine("Connected successfully");
            await ReceiveAsync();
        }
        catch (WebSocketException ex)
        {
            await Console.Error.WriteLineAsync($"WebSocket connection failed: {ex.Message}");
            if (_webSocket.CloseStatus.HasValue)
            {
                await Console.Error.WriteLineAsync($"Close Status: {_webSocket.CloseStatus}");
                await Console.Error.WriteLineAsync($"Close Description: {_webSocket.CloseStatusDescription}");
            }
            options.OnError?.Invoke(ex);
        }
    }

    private async Task ReceiveAsync()
    {
        var buffer = new byte[1024 * 32];

        while (_webSocket.State == WebSocketState.Open)
        {
            try
            {
                var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Console.WriteLine("WebSocket closed by server.");
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by server", CancellationToken.None);
                    break;
                }

                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    var cborData = buffer.Take(result.Count).ToArray();

                    using var ms = new MemoryStream(cborData);
                    var eventFrame = CBORObject.ReadSequence(ms);
                    if (eventFrame is { Length: > 0 })
                    {
                        var header = eventFrame[0];
                        var payload = eventFrame[1];
                        if (!VerifyHeader(header)) continue;
                        var ops = Operation.FromCborObject(payload["ops"][0]);

                        Console.WriteLine($"OPS is: {ops.Action.ToUpper()} {ops.Path.Split('/').First()} ");
                        
                        Cid cid = ExtractCidFromBytes(ops.Cid);

                        var digest = Multibase.EncodeRaw(MultibaseEncoding.Base32Lower, cid);
                        Console.WriteLine($"CID is: b{digest}");

                        Console.WriteLine($"at://{payload["repo"].AsString()}/{ops.Path}");

                        var blocks = payload["blocks"].GetByteString();
                        using BinaryReader blockReader = new BinaryReader(new MemoryStream(blocks));
                        uint pragma = blockReader.ReadByte();
                        var blockHeaderBytes = blockReader.ReadBytes((int)pragma);
                        
                        var blockHeader = CBORObject.DecodeFromBytes(blockHeaderBytes);
                        var headerCid = ExtractCidFromBytes(blockHeader["roots"][0].GetByteString());
                        var headerCidDigest = Multibase.EncodeRaw(MultibaseEncoding.Base32Lower, cid);

                        Console.WriteLine($"Header Cid is: b{headerCidDigest}");

                        // Nothing beyond here is working correctly
                        var blockLength = blockReader.ReadByte();
                        
                        var blockCid = ExtractCidFromBytes(blockReader.ReadBytes(37));
                        var blockCidDigest = Multibase.EncodeRaw(MultibaseEncoding.Base32Lower, blockCid);
                        if (blockCidDigest != headerCidDigest)
                        {
                            Console.WriteLine($"Block CID b{blockCidDigest} does not match header CID b{headerCidDigest}");
                        }
                        else
                        {
                            Console.WriteLine($"Block CID b{blockCidDigest} matches header CID b{headerCidDigest}");
                        }

                        var blockData = blockReader.ReadBytes(blockLength);
                        var remainingBytes = blockReader.ReadBytes((int)(blockReader.BaseStream.Length - blockReader.BaseStream.Position));
                        




                        Console.WriteLine("====================================");

                    }
                }
                else
                {
                    Console.WriteLine("Unexpected message type received.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving message: {ex.Message}");
            }
        }
    }

    private static Cid ExtractCidFromBytes(byte[] bytes)
    {
        const int MULTIHASH_LENGTH = 34; // code ID (SHA_256) + hash length + hash
        using BinaryReader reader = new BinaryReader(new MemoryStream(bytes));
        var identifier = reader.ReadByte();
        var version = reader.ReadByte();
        var codec = reader.ReadByte();
        
        // if position + length is greater than the length of the stream, we have a problem
        if (reader.BaseStream.Position + MULTIHASH_LENGTH > reader.BaseStream.Length)
        {
            throw new Exception("Multihash length exceeds stream length");
        }

        var multihash = reader.ReadBytes((int)(reader.BaseStream.Position + MULTIHASH_LENGTH));

        return new Cid((MulticodecCode)codec, Multihash.Cast(multihash));
    }

    private static bool VerifyHeader(CBORObject header)
    {
        // simple check we're not getting an error event
        return header["op"].AsInt32() == 1;
    }

    public async Task StopAsync()
    {
        if (_webSocket is { State: WebSocketState.Open })
        {
            await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Stopped by client", CancellationToken.None);
        }
    }
    
}
