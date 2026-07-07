using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Wallaby.Sinks.Http.Tests.Integration;

/// <summary>
/// In-process webhook endpoint: an <see cref="HttpListener"/> on a loopback port that records every
/// envelope (verifying the HMAC signature when a secret is expected) and returns 200.
/// </summary>
internal sealed class WebhookReceiver : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly byte[]? _signingKey;

    public WebhookReceiver(string? signingSecret = null)
    {
        _signingKey = signingSecret is null ? null : Encoding.UTF8.GetBytes(signingSecret);

        var port = FreePort();
        Endpoint = $"http://127.0.0.1:{port}/hooks/";
        _listener.Prefixes.Add(Endpoint);
        _listener.Start();
        _loop = Task.Run(ReceiveLoopAsync);
    }

    public string Endpoint { get; }

    /// <summary>Every received record element, flattened across envelopes in arrival order.</summary>
    public ConcurrentQueue<JsonElement> Records { get; } = new();

    /// <summary>True once any request arrived with a missing or invalid signature.</summary>
    public bool SawInvalidSignature { get; private set; }

    /// <summary>The latest arrived state of a document id: its record element, or null when never seen.</summary>
    public JsonElement? Latest(string id, string operation)
        => Records.LastOrDefault(r =>
            r.GetProperty("id").GetString() == id && r.GetProperty("operation").GetString() == operation) is
            { ValueKind: not JsonValueKind.Undefined } match
            ? match
            : null;

    private static int FreePort()
    {
        using var socket = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        socket.Start();
        return ((IPEndPoint)socket.LocalEndpoint).Port;
    }

    private async Task ReceiveLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await _listener.GetContextAsync().WaitAsync(_cts.Token);
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }

            using var buffer = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(buffer);
            var body = buffer.ToArray();

            if (_signingKey is not null)
            {
                var expected = $"sha256={Convert.ToHexStringLower(HMACSHA256.HashData(_signingKey, body))}";
                if (context.Request.Headers[HttpSink.SignatureHeader] != expected)
                {
                    SawInvalidSignature = true;
                }
            }

            using var envelope = JsonDocument.Parse(body);
            foreach (var record in envelope.RootElement.GetProperty("records").EnumerateArray())
            {
                Records.Enqueue(record.Clone());
            }

            context.Response.StatusCode = 200;
            context.Response.Close();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        try
        {
            await _loop;
        }
        catch (ObjectDisposedException)
        {
        }
        _listener.Close();
        _cts.Dispose();
    }
}
