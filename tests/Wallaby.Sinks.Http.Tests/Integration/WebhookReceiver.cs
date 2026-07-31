using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using StandardWebhooks;

namespace Wallaby.Sinks.Http.Tests.Integration;

/// <summary>
/// In-process webhook endpoint: an <see cref="HttpListener"/> on a loopback port that records every
/// envelope (verifying the Standard Webhooks signature via the independent StandardWebhooks package
/// when a secret is expected) and returns 200.
/// </summary>
internal sealed class WebhookReceiver : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;
    private readonly StandardWebhook? _verifier;

    public WebhookReceiver(string? signingSecret = null)
    {
        _verifier = signingSecret is null ? null : new StandardWebhook(signingSecret);

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

    /// <summary>
    /// The latest arrived state of a document id: its record element, or null when never seen.
    /// An <paramref name="action"/> narrows the match to records with that metadata action.
    /// </summary>
    public JsonElement? Latest(string id, string operation, string? action = null)
        => Records.LastOrDefault(r =>
            r.GetProperty("id").GetString() == id
            && r.GetProperty("operation").GetString() == operation
            && (action is null || r.GetProperty("metadata").GetProperty("action").GetString() == action)) is
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

            if (_verifier is not null && !VerifySignature(context.Request, body))
            {
                SawInvalidSignature = true;
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

    // The documented receiver recipe: a fresh timestamp within tolerance, and any of the signatures
    // matching the reference implementation's output for {id}.{timestamp}.{body}.
    private bool VerifySignature(HttpListenerRequest request, byte[] body)
    {
        var id = request.Headers[HttpSink.IdHeader];
        var timestamp = request.Headers[HttpSink.TimestampHeader];
        var signatures = request.Headers[HttpSink.SignatureHeader];
        if (id is null || signatures is null
            || timestamp is null
            || !long.TryParse(timestamp, out var unix)
            || Math.Abs(DateTimeOffset.UtcNow.ToUnixTimeSeconds() - unix) > 300)
        {
            return false;
        }

        var expected = _verifier!.Sign(
            id, DateTimeOffset.FromUnixTimeSeconds(unix), Encoding.UTF8.GetString(body));
        return signatures.Split(' ').Contains(expected, StringComparer.Ordinal);
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
