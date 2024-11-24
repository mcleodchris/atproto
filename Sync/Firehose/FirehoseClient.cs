using System.Net.WebSockets;
using PeterO.Cbor;
using ContentIdentifier;
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
        var buffer = new byte[1024 * 1024 * 4];

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
                        var hash = await Multihash.SumAsync(HashType.SHA2_256, ops.Cid);
                        var cid = new Cid(MulticodecCode.DagCBOR, hash);
                        var b32 = new Multiformats.Base.Base32Encoding();
                        
                        Console.WriteLine($"CID is: {b32.Encode(cid).ToLower()}");
                        
                        Console.WriteLine($"at://{payload["repo"].AsString()}/{ops.Path}");
                        
                        // var blocks = payload["blocks"];
                        // Console.WriteLine($"Blocks is: {blocks.Type}");

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