using Wallaby.Providers;

namespace Wallaby.Marten.Internal;

/// <summary>Preview placeholder: registration compiles against the seams; capture is not yet implemented.</summary>
internal sealed class MartenModelProvider : IWallabyModelProvider
{
    public CapturePlan BuildCapturePlan(CaptureSpec spec)
        => throw new NotSupportedException(
            "The Wallaby.Marten provider is a preview placeholder; capture is not yet implemented.");

    public QualifiedTable ResolveTable(Type entityClrType)
        => throw new NotSupportedException(
            "The Wallaby.Marten provider is a preview placeholder; table resolution is not yet implemented.");
}
